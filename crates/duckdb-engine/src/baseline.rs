//! Operating `qa.baseline`: see what the accepted normal is, and change it.
//!
//! #281. A gate that cannot be re-based is a gate that gets removed. When a
//! source legitimately changes shape - a new product line, a migrated system,
//! a rate that really did move - the accepted history describes a world that no
//! longer exists, and every run from then on fails. With no way to say "this is
//! the new normal", the operator's only options are to delete the node or widen
//! its thresholds until it means nothing. Both end with the check gone, and the
//! second ends with it gone while still appearing to be there.
//!
//! Two files per node, and the split is the point:
//!
//! | file                   | holds                          | written        |
//! |------------------------|--------------------------------|----------------|
//! | `<node>.json`          | the ACCEPTED profiles          | on run success |
//! | `<node>.observed.json` | the profile this run MEASURED  | always         |
//!
//! The accepted file is deferred like a watermark: a run that failed downstream
//! must not leave today's numbers as the new normal. The observed file is not,
//! because the run that gets REFUSED is exactly the one whose numbers an
//! operator needs to look at and possibly accept. Recording it only on success
//! would throw away the profile in every case where accept is the thing you
//! want.

use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::{json, Value as JsonValue};

use crate::EngineError;

/// Where a pipeline's baselines live.
pub fn dir(workspace: &Path, pipeline: &str) -> PathBuf {
    // Sanitised the same way the executor sanitises it when writing, or a
    // pipeline whose name contains a slash would be looked up in a folder that
    // does not exist and report "no baseline" for one that is right there.
    workspace
        .join("state")
        .join(crate::connectors::sanitize_path_segment(pipeline))
        .join("baselines")
}

/// The accepted history for one node.
pub fn accepted_path(workspace: &Path, pipeline: &str, node_id: &str) -> PathBuf {
    dir(workspace, pipeline)
        .join(format!("{}.json", crate::connectors::sanitize_path_segment(node_id)))
}

/// What the last run measured for one node, accepted or not.
pub fn observed_path(workspace: &Path, pipeline: &str, node_id: &str) -> PathBuf {
    dir(workspace, pipeline)
        .join(format!("{}.observed.json", crate::connectors::sanitize_path_segment(node_id)))
}

/// One node's baseline, as an operator needs to see it.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Status {
    pub pipeline: String,
    pub node: String,
    /// How many accepted profiles the median is drawn from.
    pub accepted: usize,
    /// When the last run measured this node, if one has.
    pub observed_at: Option<String>,
    /// What that run concluded: "ok", "violation", "first_run".
    pub observed_status: Option<String>,
    /// True when there is a measured profile that is not in the accepted
    /// history - i.e. there is something to accept.
    pub pending: bool,
}

fn read_json(path: &Path) -> Option<JsonValue> {
    serde_json::from_str(&std::fs::read_to_string(path).ok()?).ok()
}

fn accepted_profiles(workspace: &Path, pipeline: &str, node_id: &str) -> Vec<JsonValue> {
    read_json(&accepted_path(workspace, pipeline, node_id))
        .and_then(|v| v.get("profiles").cloned())
        .and_then(|v| serde_json::from_value(v).ok())
        .unwrap_or_default()
}

/// Record what this run measured, whatever the run then does about it.
///
/// Not deferred, and not conditional on the check passing: the refused run is
/// the one whose numbers matter.
pub fn record_observation(
    path: &Path,
    profile: &JsonValue,
    status: &str,
    violations: &[String],
) {
    if let Some(parent) = path.parent() {
        if std::fs::create_dir_all(parent).is_err() {
            return;
        }
    }
    let body = json!({
        "at": chrono::Utc::now().to_rfc3339(),
        "status": status,
        "violations": violations,
        "profile": profile,
    });
    // Best effort on purpose. Failing a run because an observation could not be
    // written would turn an operability aid into an outage.
    if let Ok(text) = serde_json::to_string_pretty(&body) {
        let _ = std::fs::write(path, text);
    }
}

/// Every node with a baseline, across every pipeline in the workspace.
pub fn list(workspace: &Path) -> Vec<Status> {
    let mut out = Vec::new();
    let state = workspace.join("state");
    let Ok(pipelines) = std::fs::read_dir(&state) else {
        return out;
    };
    for p in pipelines.flatten() {
        let pipeline = p.file_name().to_string_lossy().to_string();
        let Ok(entries) = std::fs::read_dir(dir(workspace, &pipeline)) else {
            continue;
        };
        for e in entries.flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            // The observed file is a sibling of the accepted one; listing both
            // would report every node twice.
            if !name.ends_with(".json") || name.ends_with(".observed.json") {
                continue;
            }
            let node = name.trim_end_matches(".json").to_string();
            out.push(status_of(workspace, &pipeline, &node));
        }
    }
    out.sort_by(|a, b| (&a.pipeline, &a.node).cmp(&(&b.pipeline, &b.node)));
    out
}

fn status_of(workspace: &Path, pipeline: &str, node: &str) -> Status {
    let observed = read_json(&observed_path(workspace, pipeline, node));
    let accepted = accepted_profiles(workspace, pipeline, node);
    let observed_at = observed
        .as_ref()
        .and_then(|o| o.get("at"))
        .and_then(JsonValue::as_str)
        .map(str::to_string);
    let observed_status = observed
        .as_ref()
        .and_then(|o| o.get("status"))
        .and_then(JsonValue::as_str)
        .map(str::to_string);
    // Something to accept means: a profile was measured, and it is not already
    // the most recent accepted one.
    let pending = match (observed.as_ref().and_then(|o| o.get("profile")), accepted.last()) {
        (Some(obs), Some(last)) => obs != last,
        (Some(_), None) => true,
        _ => false,
    };
    Status {
        pipeline: pipeline.to_string(),
        node: node.to_string(),
        accepted: accepted.len(),
        observed_at,
        observed_status,
        pending,
    }
}

/// One metric, as the accepted history and the last run each see it.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricView {
    pub metric: String,
    /// The median of the accepted history - what a rule is compared against.
    pub baseline: Option<f64>,
    pub observed: Option<f64>,
    pub change_pct: Option<f64>,
}

/// The detail behind one node: what would change if this were accepted.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Inspection {
    #[serde(flatten)]
    pub status: Status,
    pub violations: Vec<String>,
    pub metrics: Vec<MetricView>,
}

/// The median of the accepted history for one metric.
///
/// Median rather than mean, matching the check itself: one odd day must not
/// drag the baseline toward itself, and an operator comparing against a
/// different number than the gate uses would be reading a different question.
fn median(profiles: &[JsonValue], key: &str) -> Option<f64> {
    let mut vals: Vec<f64> = profiles
        .iter()
        .filter_map(|p| p.get(key))
        .filter_map(JsonValue::as_f64)
        .collect();
    if vals.is_empty() {
        return None;
    }
    vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = vals.len();
    Some(if n % 2 == 1 {
        vals[n / 2]
    } else {
        (vals[n / 2 - 1] + vals[n / 2]) / 2.0
    })
}

pub fn inspect(workspace: &Path, pipeline: &str, node: &str) -> Inspection {
    let accepted = accepted_profiles(workspace, pipeline, node);
    let observed = read_json(&observed_path(workspace, pipeline, node));
    let obs_profile = observed.as_ref().and_then(|o| o.get("profile")).cloned();
    let violations: Vec<String> = observed
        .as_ref()
        .and_then(|o| o.get("violations"))
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_default();

    // Every metric either side knows about, so a metric that only appears in
    // one of them is visible rather than silently dropped.
    let mut keys: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for p in &accepted {
        if let Some(o) = p.as_object() {
            keys.extend(o.keys().cloned());
        }
    }
    if let Some(o) = obs_profile.as_ref().and_then(JsonValue::as_object) {
        keys.extend(o.keys().cloned());
    }

    let metrics = keys
        .into_iter()
        .map(|metric| {
            let baseline = median(&accepted, &metric);
            let observed =
                obs_profile.as_ref().and_then(|p| p.get(&metric)).and_then(JsonValue::as_f64);
            let change_pct = match (baseline, observed) {
                (Some(b), Some(c)) if b != 0.0 => Some((c - b) / b * 100.0),
                _ => None,
            };
            MetricView { metric, baseline, observed, change_pct }
        })
        .collect();

    Inspection { status: status_of(workspace, pipeline, node), violations, metrics }
}

/// A one-line summary of an accepted history, for the audit log.
fn describe(profiles: &[JsonValue]) -> String {
    match profiles.len() {
        0 => "no accepted profiles".to_string(),
        n => {
            let rows = median(profiles, "row_count")
                .map(|v| format!(", median row_count {v}"))
                .unwrap_or_default();
            format!("{n} accepted profile(s){rows}")
        }
    }
}

/// Promote the last measured profile to the accepted baseline.
///
/// `history` caps how many are kept, matching the node's own setting so an
/// accept cannot grow the history past what the check will read.
pub fn accept(
    workspace: &Path,
    pipeline: &str,
    node: &str,
    history: usize,
) -> Result<Inspection, EngineError> {
    crate::policy::state_mutation_allowed(workspace)?;

    let observed = read_json(&observed_path(workspace, pipeline, node)).ok_or_else(|| {
        EngineError::Config(format!(
            "baseline: {pipeline}/{node} has no measured profile to accept. Run the pipeline \
             first - accepting is promoting what a run saw, not inventing a number."
        ))
    })?;
    let profile = observed.get("profile").cloned().ok_or_else(|| {
        EngineError::Config(format!("baseline: {pipeline}/{node} observation has no profile"))
    })?;

    let before = accepted_profiles(workspace, pipeline, node);
    let mut kept = before.clone();
    kept.push(profile);
    let keep_from = kept.len().saturating_sub(history.max(1));
    let kept: Vec<JsonValue> = kept[keep_from..].to_vec();

    let path = accepted_path(workspace, pipeline, node);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| EngineError::Config(format!("baseline: {}: {e}", parent.display())))?;
    }
    let text = serde_json::to_string_pretty(&json!({ "profiles": kept }))
        .map_err(|e| EngineError::Config(format!("baseline: serialize: {e}")))?;
    write_atomically(&path, &text)?;

    crate::audit::note(
        workspace,
        "baseline.accept",
        &format!("{pipeline}/{node}"),
        Some(format!("was {} - now {}", describe(&before), describe(&kept))),
    );
    Ok(inspect(workspace, pipeline, node))
}

/// Forget the accepted history, so the next run starts it over.
///
/// The observation is left alone: it describes a run that happened, and
/// deleting it would remove the evidence somebody is about to look at.
pub fn clear(workspace: &Path, pipeline: &str, node: &str) -> Result<usize, EngineError> {
    crate::policy::state_mutation_allowed(workspace)?;
    let before = accepted_profiles(workspace, pipeline, node);
    let path = accepted_path(workspace, pipeline, node);
    match std::fs::remove_file(&path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(EngineError::Config(format!("baseline: {}: {e}", path.display()))),
    }
    crate::audit::note(
        workspace,
        "baseline.clear",
        &format!("{pipeline}/{node}"),
        Some(format!("cleared {}", describe(&before))),
    );
    Ok(before.len())
}

/// Write via a temp file and rename, so a run reading this never sees a
/// half-written history. On Windows rename replaces the destination, so
/// removing it first would only open a window where it does not exist.
fn write_atomically(path: &Path, text: &str) -> Result<(), EngineError> {
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, text)
        .map_err(|e| EngineError::Config(format!("baseline: write {}: {e}", tmp.display())))?;
    std::fs::rename(&tmp, path).map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        EngineError::Config(format!("baseline: rename into {}: {e}", path.display()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ws() -> tempfile::TempDir {
        tempfile::tempdir().unwrap()
    }

    fn seed(dir: &Path, pipeline: &str, node: &str, accepted: &[f64], observed: Option<f64>) {
        let d = super::dir(dir, pipeline);
        std::fs::create_dir_all(&d).unwrap();
        let profiles: Vec<JsonValue> = accepted.iter().map(|v| json!({ "row_count": v })).collect();
        std::fs::write(
            accepted_path(dir, pipeline, node),
            serde_json::to_string(&json!({ "profiles": profiles })).unwrap(),
        )
        .unwrap();
        if let Some(o) = observed {
            std::fs::write(
                observed_path(dir, pipeline, node),
                serde_json::to_string(&json!({
                    "at": "2026-08-28T00:00:00Z",
                    "status": "violation",
                    "violations": ["row_count fell 84%"],
                    "profile": { "row_count": o },
                }))
                .unwrap(),
            )
            .unwrap();
        }
    }

    /// The headline case from the issue: the source legitimately changed, and
    /// the operator needs a way to say so without removing the check.
    #[test]
    fn accepting_makes_the_measured_profile_the_new_normal() {
        let t = ws();
        seed(t.path(), "orders", "q", &[5_120_310.0, 5_131_244.0, 5_129_991.0], Some(842_114.0));

        let before = inspect(t.path(), "orders", "q");
        assert_eq!(before.status.accepted, 3);
        assert!(before.status.pending, "there is a measured profile that is not accepted");
        let rc = before.metrics.iter().find(|m| m.metric == "row_count").unwrap();
        assert_eq!(rc.baseline, Some(5_129_991.0), "median of the accepted three");
        assert_eq!(rc.observed, Some(842_114.0));

        let after = accept(t.path(), "orders", "q", 10).expect("accept");
        assert_eq!(after.status.accepted, 4, "the measured profile joined the history");
        assert!(!after.status.pending, "nothing left to accept");
        // The median moved toward the new value rather than jumping to it,
        // which is the point of a median baseline.
        let rc = after.metrics.iter().find(|m| m.metric == "row_count").unwrap();
        assert_eq!(rc.baseline, Some((5_120_310.0 + 5_129_991.0) / 2.0));
    }

    /// Accepting cannot grow the history past what the check reads, or the
    /// median an operator was shown is not the one the gate will use.
    #[test]
    fn accept_honours_the_history_cap() {
        let t = ws();
        seed(t.path(), "p", "n", &[1.0, 2.0, 3.0], Some(4.0));
        let after = accept(t.path(), "p", "n", 2).expect("accept");
        assert_eq!(after.status.accepted, 2, "capped at the node's history setting");
        let rc = after.metrics.iter().find(|m| m.metric == "row_count").unwrap();
        assert_eq!(rc.baseline, Some(3.5), "the two kept are the newest: 3 and 4");
    }

    /// Accepting without a measured profile is a refusal, not an invention.
    #[test]
    fn accept_refuses_when_no_run_has_measured_anything() {
        let t = ws();
        seed(t.path(), "p", "n", &[1.0], None);
        let err = accept(t.path(), "p", "n", 10).unwrap_err().to_string();
        assert!(err.contains("no measured profile"), "{err}");
    }

    /// Clearing forgets the accepted history and keeps the evidence.
    #[test]
    fn clear_forgets_the_history_but_not_the_observation() {
        let t = ws();
        seed(t.path(), "p", "n", &[1.0, 2.0], Some(9.0));
        let dropped = clear(t.path(), "p", "n").expect("clear");
        assert_eq!(dropped, 2, "reports what it dropped");
        assert!(!accepted_path(t.path(), "p", "n").exists());
        assert!(
            observed_path(t.path(), "p", "n").exists(),
            "the observation is what the operator is looking at - it must survive"
        );
        let after = inspect(t.path(), "p", "n");
        assert_eq!(after.status.accepted, 0);
        assert!(after.status.pending, "the measured profile is still there to accept");
    }

    /// Both mutations are recorded with what actually changed, because
    /// "someone cleared it" is not reviewable and the value it held is.
    #[test]
    fn both_mutations_are_audited_with_the_before_and_after() {
        let t = ws();
        seed(t.path(), "orders", "q", &[10.0, 20.0], Some(30.0));
        accept(t.path(), "orders", "q", 10).expect("accept");
        clear(t.path(), "orders", "q").expect("clear");

        let log = std::fs::read_to_string(crate::audit::audit_path(t.path())).expect("audit log");
        let lines: Vec<crate::audit::Entry> =
            log.lines().filter_map(|l| serde_json::from_str(l).ok()).collect();
        assert_eq!(lines.len(), 2, "one line per mutation: {log}");

        assert_eq!(lines[0].action, "baseline.accept");
        assert_eq!(lines[0].target, "orders/q");
        let d = lines[0].detail.clone().unwrap_or_default();
        assert!(d.contains("was 2 accepted"), "names what it replaced: {d}");
        assert!(d.contains("now 3 accepted"), "and what it became: {d}");

        assert_eq!(lines[1].action, "baseline.clear");
        let d = lines[1].detail.clone().unwrap_or_default();
        assert!(d.contains("cleared 3 accepted"), "says what was lost: {d}");
        assert!(!lines[1].at.is_empty(), "and when");
    }

    /// Listing reports every node across pipelines and does not count the
    /// observation file as a second node.
    #[test]
    fn listing_reports_each_node_once() {
        let t = ws();
        seed(t.path(), "a", "n1", &[1.0], Some(2.0));
        seed(t.path(), "b", "n2", &[1.0], None);
        let rows = list(t.path());
        assert_eq!(rows.len(), 2, "{rows:?}");
        assert_eq!((rows[0].pipeline.as_str(), rows[0].node.as_str()), ("a", "n1"));
        assert!(rows[0].pending);
        assert!(!rows[1].pending, "no observation means nothing to accept");
    }
}
