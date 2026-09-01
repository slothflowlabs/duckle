//! #297: what exactly is running, and how do we put it back.
//!
//! A workspace's control plane is several coupled files - pipelines, plans,
//! schedules, parameter contracts. Updating them one at a time means there is a
//! moment when half the new state is live, and no way afterwards to say which
//! tested version an operator actually has.
//!
//! A release is an immutable, hash-addressed record of all of it, and an
//! environment points at one. Activating swaps the pointer with a single
//! rename, so a reader sees the previous complete release or the new complete
//! release and never a half-copied workspace.
//!
//! ## It records, it does not copy
//!
//! A release names content by hash rather than holding a copy of it. That keeps
//! the record small, makes "is what is on disk still the release we activated"
//! answerable, and means promoting the same release through environments cannot
//! rebuild anything - which is the property #297 asks for and the reason not to
//! store a tarball instead.
//!
//! ## The hashes are the ones already in use
//!
//! Pipeline hashes come from [`crate::retry::pipeline_hash`], the same function
//! a run receipt records. A second hash would eventually disagree with the
//! first about whether a pipeline changed, and then no answer could be trusted.
//!
//! ## No secrets
//!
//! Only hashes and declarations. Connection *references* are recorded because
//! activation has to check they resolve; the values behind them are not, and
//! nothing here reads them.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// Bumped when the document's shape changes, so a newer release read by an
/// older build fails loudly rather than being half understood.
pub const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct Body {
    pub schema_version: u32,
    /// The commit the workspace was on, when it is a git workspace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub git_commit: Option<String>,
    /// The pipeline format this workspace is written in (#299).
    pub format_version: u32,
    /// pipeline id -> sha256 of the parsed document.
    pub pipelines: BTreeMap<String, String>,
    /// The parameter contract each pipeline declares (#317), so a release
    /// records what it can be given as well as what it does.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub parameters: BTreeMap<String, crate::params::Schema>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plans_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedules_hash: Option<String>,
    /// Every saved connection any pipeline names. Recorded so activation can
    /// refuse before mutating when one is missing; the values are not here.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub connection_refs: BTreeSet<String>,
}

/// An immutable release. `id` is the hash of everything else.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Release {
    pub id: String,
    #[serde(flatten)]
    pub body: Body,
}

fn sha256(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(bytes);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

/// The id of a body: sha256 over its canonical serialization.
///
/// Canonical because every map in it is a `BTreeMap` and every set a
/// `BTreeSet`, so the bytes depend on the content and not on the order a
/// directory happened to be read in. Two builds of an unchanged workspace
/// produce the same id, which is what makes "has anything changed?" a
/// comparison rather than an investigation.
pub fn id_of(body: &Body) -> String {
    sha256(&serde_json::to_vec(body).unwrap_or_default())
}

fn file_hash(path: &Path) -> Option<String> {
    std::fs::read(path).ok().map(|b| sha256(&b))
}

/// Every `connectionRef` any node names.
fn connection_refs(doc: &serde_json::Value) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let Some(nodes) = doc.get("nodes").and_then(|n| n.as_array()) else { return out };
    for node in nodes {
        if let Some(r) = node
            .get("data")
            .and_then(|d| d.get("properties"))
            .and_then(|p| p.get("connectionRef"))
            .and_then(|v| v.as_str())
        {
            if !r.trim().is_empty() {
                out.insert(r.to_string());
            }
        }
    }
    out
}

/// Record the workspace as it is now.
///
/// Refuses a workspace in a format this build cannot fully read: a release is a
/// claim about what is there, and one built from a document half of whose
/// settings were invisible would be a confident wrong claim.
pub fn build(workspace: &Path) -> Result<Release, String> {
    let docs = crate::catalog::documents(workspace);
    let mut body = Body { schema_version: SCHEMA_VERSION, ..Default::default() };
    for (id, doc) in &docs {
        crate::format::check(doc).map_err(|e| format!("{id}: {e}"))?;
        body.format_version = body.format_version.max(crate::format::version_of(doc));
        let parsed: crate::PipelineDoc = serde_json::from_value(doc.clone())
            .map_err(|e| format!("{id}: {e}"))?;
        body.pipelines.insert(id.clone(), crate::retry::pipeline_hash(&parsed));
        if !parsed.parameters.is_empty() {
            body.parameters.insert(id.clone(), parsed.parameters.clone());
        }
        body.connection_refs.extend(connection_refs(doc));
    }
    body.plans_hash = file_hash(&crate::plans::plans_path(workspace));
    body.schedules_hash = file_hash(&crate::schedules::schedules_path(workspace));
    body.git_commit = git_commit(workspace);
    let id = id_of(&body);
    Ok(Release { id, body })
}

fn git_commit(workspace: &Path) -> Option<String> {
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(workspace)
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
        .filter(|s| !s.is_empty())
}

pub fn dir(workspace: &Path) -> PathBuf {
    workspace.join(".duckle").join("releases")
}

fn path_for(workspace: &Path, id: &str) -> PathBuf {
    dir(workspace).join(format!("{id}.json"))
}

pub fn env_dir(workspace: &Path, environment: &str) -> PathBuf {
    workspace.join(".duckle").join("environments").join(environment)
}

/// Write a release, and refuse to change one that already exists.
///
/// Immutability is enforced here rather than assumed: a release is what a run
/// is traced back to, and one that could be edited after the fact would make
/// every trace unfalsifiable.
pub fn save(workspace: &Path, release: &Release) -> Result<PathBuf, String> {
    let path = path_for(workspace, &release.id);
    if path.exists() {
        let existing = load(workspace, &release.id)?;
        if existing != *release {
            return Err(format!(
                "release {} already exists with different content - a release id is its hash, so this cannot happen honestly",
                release.id
            ));
        }
        // Rebuilding an unchanged workspace produces the same id and the same
        // bytes. That is not an error, it is the point.
        return Ok(path);
    }
    std::fs::create_dir_all(dir(workspace)).map_err(|e| e.to_string())?;
    let body = serde_json::to_string_pretty(release).map_err(|e| e.to_string())?;
    std::fs::write(&path, body).map_err(|e| e.to_string())?;
    Ok(path)
}

pub fn load(workspace: &Path, id: &str) -> Result<Release, String> {
    let path = path_for(workspace, id);
    let text = std::fs::read_to_string(&path)
        .map_err(|e| format!("{}: {e}", path.display()))?;
    serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))
}

/// The release an environment is running, if any.
pub fn active(workspace: &Path, environment: &str) -> Option<String> {
    std::fs::read_to_string(env_dir(workspace, environment).join("active"))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// The release this environment was running before the current one.
pub fn previous(workspace: &Path, environment: &str) -> Option<String> {
    std::fs::read_to_string(env_dir(workspace, environment).join("previous"))
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Point an environment at a release.
///
/// The current id becomes `previous` first, then `active` is replaced by a
/// single rename. **Never remove-then-rename**: `std::fs::rename` is
/// `MoveFileExW` with `MOVEFILE_REPLACE_EXISTING` and replaces the destination
/// even while a reader holds it open, so unlinking first buys nothing and costs
/// exactly the guarantee this function exists for - a window in which the
/// environment points at nothing at all.
pub fn point_at(workspace: &Path, environment: &str, id: &str) -> Result<(), String> {
    let dir = env_dir(workspace, environment);
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    if let Some(current) = active(workspace, environment) {
        if current == id {
            return Ok(());
        }
        // Written before the swap, so a crash between the two leaves the
        // environment on its old release with a correct `previous`, rather than
        // on the new one with no way back.
        let tmp = dir.join("previous.tmp");
        std::fs::write(&tmp, &current).map_err(|e| e.to_string())?;
        std::fs::rename(&tmp, dir.join("previous")).map_err(|e| e.to_string())?;
    }
    let tmp = dir.join("active.tmp");
    std::fs::write(&tmp, id).map_err(|e| e.to_string())?;
    std::fs::rename(&tmp, dir.join("active")).map_err(|e| e.to_string())
}

/// What changed between two releases.
#[derive(Debug, Clone, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct Diff {
    pub added: Vec<String>,
    pub changed: Vec<String>,
    pub removed: Vec<String>,
    pub plans_changed: bool,
    pub schedules_changed: bool,
    /// Pipelines whose declared parameter contract differs. Called out
    /// separately because a caller that was passing a value the new contract
    /// refuses breaks at the next run rather than at activation.
    pub parameters_changed: Vec<String>,
    /// Connections the new release needs and the old one did not.
    pub new_connection_refs: Vec<String>,
    pub format_version: (u32, u32),
}

impl Diff {
    pub fn is_empty(&self) -> bool {
        self.added.is_empty()
            && self.changed.is_empty()
            && self.removed.is_empty()
            && !self.plans_changed
            && !self.schedules_changed
            && self.parameters_changed.is_empty()
    }
}

pub fn diff(from: &Body, to: &Body) -> Diff {
    let mut d = Diff {
        plans_changed: from.plans_hash != to.plans_hash,
        // Schedules are part of the control plane and change what runs when,
        // so a release diff that ignored them would show "nothing changed" for
        // a promotion that silently rescheduled every pipeline.
        schedules_changed: from.schedules_hash != to.schedules_hash,
        format_version: (from.format_version, to.format_version),
        ..Default::default()
    };
    for (id, hash) in &to.pipelines {
        match from.pipelines.get(id) {
            None => d.added.push(id.clone()),
            Some(before) if before != hash => d.changed.push(id.clone()),
            Some(_) => {}
        }
    }
    for id in from.pipelines.keys() {
        if !to.pipelines.contains_key(id) {
            d.removed.push(id.clone());
        }
    }
    let names: BTreeSet<&String> = from.parameters.keys().chain(to.parameters.keys()).collect();
    for name in names {
        if from.parameters.get(name) != to.parameters.get(name) {
            d.parameters_changed.push(name.clone());
        }
    }
    d.new_connection_refs = to
        .connection_refs
        .difference(&from.connection_refs)
        .cloned()
        .collect();
    d
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ws(files: &[(&str, &str)]) -> tempfile::TempDir {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("pipelines")).unwrap();
        for (name, body) in files {
            std::fs::write(tmp.path().join("pipelines").join(name), body).unwrap();
        }
        tmp
    }

    const ONE: &str = r#"{"formatVersion":1,"name":"one","nodes":[
        {"id":"s","type":"source","position":{"x":0,"y":0},
         "data":{"label":"In","componentId":"src.csv","properties":{"path":"a.csv"}}}],
        "edges":[]}"#;

    #[test]
    fn a_release_is_its_own_hash_and_an_unchanged_workspace_rebuilds_to_it() {
        let tmp = ws(&[("one.json", ONE)]);
        let a = build(tmp.path()).unwrap();
        let b = build(tmp.path()).unwrap();
        assert_eq!(a.id, b.id, "an unchanged workspace must produce the same release");
        assert_eq!(a.id, id_of(&a.body), "the id must be the hash of the body");
        assert_eq!(a.id.len(), 64);
    }

    #[test]
    fn a_changed_pipeline_changes_the_release_id() {
        let tmp = ws(&[("one.json", ONE)]);
        let before = build(tmp.path()).unwrap();
        std::fs::write(
            tmp.path().join("pipelines/one.json"),
            ONE.replace("a.csv", "b.csv"),
        )
        .unwrap();
        let after = build(tmp.path()).unwrap();
        assert_ne!(before.id, after.id);
        let d = diff(&before.body, &after.body);
        assert_eq!(d.changed, vec!["one"]);
        assert!(d.added.is_empty() && d.removed.is_empty());
    }

    #[test]
    fn a_schedule_change_is_part_of_the_release() {
        // Schedules decide what runs when. A release diff that ignored them
        // would report "nothing changed" for a promotion that rescheduled
        // everything.
        let tmp = ws(&[("one.json", ONE)]);
        let before = build(tmp.path()).unwrap();
        std::fs::write(tmp.path().join("schedules.json"), "[]").unwrap();
        let after = build(tmp.path()).unwrap();
        assert_ne!(before.id, after.id);
        assert!(diff(&before.body, &after.body).schedules_changed);
    }

    #[test]
    fn a_release_cannot_be_rewritten() {
        // A release is what a run is traced back to. One that could be edited
        // afterwards would make every trace unfalsifiable.
        let tmp = ws(&[("one.json", ONE)]);
        let mut r = build(tmp.path()).unwrap();
        save(tmp.path(), &r).unwrap();
        // Saving the same thing again is fine - rebuilding an unchanged
        // workspace is expected to be a no-op.
        assert!(save(tmp.path(), &r).is_ok());
        r.body.pipelines.insert("smuggled".into(), "deadbeef".into());
        let e = save(tmp.path(), &r).unwrap_err();
        assert!(e.contains("already exists"), "{e}");
    }

    #[test]
    fn activation_records_what_it_replaced_so_rollback_has_somewhere_to_go() {
        let tmp = ws(&[("one.json", ONE)]);
        point_at(tmp.path(), "production", "release-a").unwrap();
        assert_eq!(active(tmp.path(), "production").as_deref(), Some("release-a"));
        assert_eq!(previous(tmp.path(), "production"), None, "nothing to go back to yet");

        point_at(tmp.path(), "production", "release-b").unwrap();
        assert_eq!(active(tmp.path(), "production").as_deref(), Some("release-b"));
        assert_eq!(previous(tmp.path(), "production").as_deref(), Some("release-a"));

        // Rolling back is pointing at the previous one, which then makes the
        // one just left the thing to come back to.
        point_at(tmp.path(), "production", "release-a").unwrap();
        assert_eq!(active(tmp.path(), "production").as_deref(), Some("release-a"));
        assert_eq!(previous(tmp.path(), "production").as_deref(), Some("release-b"));
    }

    #[test]
    fn re_activating_the_same_release_does_not_lose_the_one_before_it() {
        // Otherwise `previous` becomes the current release and rollback is a
        // no-op exactly when someone needs it.
        let tmp = ws(&[("one.json", ONE)]);
        point_at(tmp.path(), "prod", "a").unwrap();
        point_at(tmp.path(), "prod", "b").unwrap();
        point_at(tmp.path(), "prod", "b").unwrap();
        assert_eq!(previous(tmp.path(), "prod").as_deref(), Some("a"));
    }

    #[test]
    fn environments_point_independently() {
        let tmp = ws(&[("one.json", ONE)]);
        point_at(tmp.path(), "staging", "b").unwrap();
        point_at(tmp.path(), "production", "a").unwrap();
        assert_eq!(active(tmp.path(), "staging").as_deref(), Some("b"));
        assert_eq!(active(tmp.path(), "production").as_deref(), Some("a"));
    }

    #[test]
    fn a_workspace_this_build_cannot_fully_read_yields_no_release() {
        // A release is a claim about what is there. One built from a document
        // half of whose settings were invisible would be a confident wrong
        // claim.
        let tmp = ws(&[("future.json", r#"{"formatVersion":99,"nodes":[],"edges":[]}"#)]);
        let e = build(tmp.path()).unwrap_err();
        assert!(e.contains("format version"), "{e}");
    }

    #[test]
    fn the_connections_a_release_needs_are_recorded_but_never_their_values() {
        let doc = r#"{"formatVersion":1,"name":"c","nodes":[
            {"id":"s","type":"source","position":{"x":0,"y":0},
             "data":{"label":"In","componentId":"src.postgres",
                     "properties":{"connectionRef":"warehouse","password":"hunter2"}}}],
            "edges":[]}"#;
        let tmp = ws(&[("c.json", doc)]);
        let r = build(tmp.path()).unwrap();
        assert!(r.body.connection_refs.contains("warehouse"));
        let rendered = serde_json::to_string(&r).unwrap();
        assert!(!rendered.contains("hunter2"), "a secret reached the release document");
    }

    #[test]
    fn a_parameter_contract_change_is_called_out_on_its_own() {
        // A caller passing a value the new contract refuses breaks at the next
        // run rather than at activation, so it is worth naming.
        let with = r#"{"formatVersion":1,"name":"p","parameters":{"region":{"type":"string"}},
            "nodes":[{"id":"s","type":"source","position":{"x":0,"y":0},
            "data":{"label":"In","componentId":"src.csv","properties":{"path":"a.csv"}}}],"edges":[]}"#;
        let tmp = ws(&[("p.json", ONE)]);
        let before = build(tmp.path()).unwrap();
        std::fs::write(tmp.path().join("pipelines/p.json"), with).unwrap();
        let after = build(tmp.path()).unwrap();
        let d = diff(&before.body, &after.body);
        assert_eq!(d.parameters_changed, vec!["p"]);
    }
}
