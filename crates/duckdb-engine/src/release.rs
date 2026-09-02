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
//! ## It stores the content, not only its hash
//!
//! The first version of this recorded hashes and no content, which made
//! rollback a lie: the pointer moved back to A while the workspace files still
//! held B, so the environment was not running A and `releaseId: A` on a run
//! meant only "A was the pointer when this started". A release now holds an
//! immutable copy of every control-plane file, content-addressed and shared
//! between releases, and activation MATERIALISES it. That is what makes
//! `activate A` / `activate B` / `rollback` actually execute A, B, A.
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
    /// Workspace-relative path -> sha256 of the bytes stored for it.
    ///
    /// This is what makes a release executable rather than merely descriptive.
    /// The bytes live in a content-addressed store shared between releases, so
    /// two releases differing in one pipeline cost one extra object rather than
    /// a second copy of everything.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub files: BTreeMap<String, String>,
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
    // The bytes, not only their hashes. Without this, activating a release is
    // just moving a pointer and rolling back leaves the workspace on whatever
    // it happened to contain.
    for (rel, path) in control_plane_files(workspace) {
        let bytes = std::fs::read(&path).map_err(|e| format!("{}: {e}", path.display()))?;
        body.files.insert(rel, put_object(workspace, &bytes)?);
    }
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

/// Where the immutable bytes live, keyed by their own hash.
///
/// Shared between releases: two releases differing in one pipeline cost one
/// extra object, not a second copy of the workspace. An object is never
/// rewritten, because its name IS its content.
pub fn objects_dir(workspace: &Path) -> PathBuf {
    dir(workspace).join("objects")
}

fn put_object(workspace: &Path, bytes: &[u8]) -> Result<String, String> {
    let hash = sha256(bytes);
    let dir = objects_dir(workspace);
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    let path = dir.join(&hash);
    if path.exists() {
        return Ok(hash);
    }
    // Temp then rename, so a reader never sees a half-written object under a
    // name that promises those exact bytes.
    let tmp = dir.join(format!(".{hash}.tmp"));
    std::fs::write(&tmp, bytes).map_err(|e| e.to_string())?;
    std::fs::rename(&tmp, &path).map_err(|e| e.to_string())?;
    Ok(hash)
}

pub fn get_object(workspace: &Path, hash: &str) -> Result<Vec<u8>, String> {
    let path = objects_dir(workspace).join(hash);
    let bytes = std::fs::read(&path).map_err(|e| format!("{}: {e}", path.display()))?;
    // Verified on the way out, not assumed. An object whose bytes no longer
    // hash to its name is corruption, and materialising it would put content
    // into the workspace that no release actually describes.
    let actual = sha256(&bytes);
    if actual != hash {
        return Err(format!(
            "release object {hash} has been altered - its content now hashes to {actual}"
        ));
    }
    Ok(bytes)
}

/// The control-plane files a release captures.
///
/// Every pipeline, plus plans and schedules when they exist. These are what
/// decide what runs and when; anything else in a workspace is data or working
/// state and is deliberately not part of the release.
fn control_plane_files(workspace: &Path) -> Vec<(String, PathBuf)> {
    let mut out: Vec<(String, PathBuf)> = crate::catalog::document_paths(workspace)
        .into_iter()
        .filter_map(|(_, path, _)| {
            let rel = path.strip_prefix(workspace).ok()?;
            Some((rel.to_string_lossy().replace('\\', "/"), path.clone()))
        })
        .collect();
    for p in [crate::plans::plans_path(workspace), crate::schedules::schedules_path(workspace)] {
        if p.exists() {
            if let Ok(rel) = p.strip_prefix(workspace) {
                out.push((rel.to_string_lossy().replace('\\', "/"), p.clone()));
            }
        }
    }
    out.sort();
    out
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

/// What the workspace holds now that this release does not, and vice versa.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Drift {
    /// Files whose bytes differ from the release's.
    pub changed: Vec<String>,
    /// Control-plane files present in the workspace and not in the release.
    /// Left in place, they mean the environment is not running this release -
    /// a schedule could fire a pipeline the release does not contain.
    pub extra: Vec<String>,
}

impl Drift {
    pub fn is_empty(&self) -> bool {
        self.changed.is_empty() && self.extra.is_empty()
    }
}

/// How the workspace differs from a release, without changing anything.
pub fn drift(workspace: &Path, release: &Release) -> Drift {
    let mut d = Drift::default();
    let on_disk: std::collections::BTreeMap<String, PathBuf> =
        control_plane_files(workspace).into_iter().collect();
    for (rel, hash) in &release.body.files {
        match on_disk.get(rel) {
            None => d.changed.push(rel.clone()),
            Some(path) => {
                let same = std::fs::read(path).map(|b| sha256(&b) == *hash).unwrap_or(false);
                if !same {
                    d.changed.push(rel.clone());
                }
            }
        }
    }
    for rel in on_disk.keys() {
        if !release.body.files.contains_key(rel) {
            d.extra.push(rel.clone());
        }
    }
    d
}

/// Write the release's content into the workspace.
///
/// This is what "activate" means. Without it, activation moved a pointer while
/// the files stayed as they were, so rolling back to A left the workspace
/// running B and `releaseId: A` on a run meant only that A was the pointer.
///
/// Files the release does not contain are REMOVED, because a pipeline left
/// behind is one a schedule can still fire - the environment would not be
/// running the release. Everything else in the workspace is untouched: this
/// moves control-plane files, not data.
pub fn materialise(workspace: &Path, release: &Release) -> Result<Vec<String>, String> {
    let mut touched = Vec::new();
    // Every object is read and verified BEFORE anything is written, so a
    // corrupt or missing one aborts with the workspace untouched rather than
    // half-way through.
    let mut staged: Vec<(PathBuf, Vec<u8>)> = Vec::new();
    for (rel, hash) in &release.body.files {
        let bytes = get_object(workspace, hash)
            .map_err(|e| format!("{rel}: {e}"))?;
        staged.push((workspace.join(rel), bytes));
    }
    for (path, bytes) in staged {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        let tmp = path.with_extension("release.tmp");
        std::fs::write(&tmp, &bytes).map_err(|e| format!("{}: {e}", path.display()))?;
        std::fs::rename(&tmp, &path).map_err(|e| format!("{}: {e}", path.display()))?;
        touched.push(path.display().to_string());
    }
    for rel in drift(workspace, release).extra {
        let path = workspace.join(&rel);
        std::fs::remove_file(&path).map_err(|e| format!("{}: {e}", path.display()))?;
        touched.push(format!("removed {rel}"));
    }
    Ok(touched)
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
    fn a_release_stores_the_bytes_and_not_only_their_hashes() {
        // Without the content, activating is moving a pointer and rolling back
        // leaves the workspace on whatever it happened to contain.
        let tmp = ws(&[("one.json", ONE)]);
        let r = build(tmp.path()).unwrap();
        assert!(!r.body.files.is_empty(), "a release with no content is not executable");
        let (rel, hash) = r.body.files.iter().next().unwrap();
        assert!(rel.ends_with("one.json"), "{rel}");
        let bytes = get_object(tmp.path(), hash).unwrap();
        assert_eq!(String::from_utf8_lossy(&bytes), ONE);
    }

    /// The defect Louis found: activate A, edit to B, activate B, roll back -
    /// and the workspace must hold A again, not B.
    #[test]
    fn rolling_back_restores_the_content_and_not_only_the_pointer() {
        let tmp = ws(&[("one.json", ONE)]);
        let a = build(tmp.path()).unwrap();
        save(tmp.path(), &a).unwrap();
        materialise(tmp.path(), &a).unwrap();
        point_at(tmp.path(), "production", &a.id).unwrap();

        let edited = ONE.replace("a.csv", "b.csv");
        std::fs::write(tmp.path().join("pipelines/one.json"), &edited).unwrap();
        let b = build(tmp.path()).unwrap();
        save(tmp.path(), &b).unwrap();
        materialise(tmp.path(), &b).unwrap();
        point_at(tmp.path(), "production", &b.id).unwrap();
        assert!(std::fs::read_to_string(tmp.path().join("pipelines/one.json"))
            .unwrap()
            .contains("b.csv"));

        // Roll back.
        let previous = previous(tmp.path(), "production").unwrap();
        assert_eq!(previous, a.id);
        materialise(tmp.path(), &load(tmp.path(), &previous).unwrap()).unwrap();
        point_at(tmp.path(), "production", &previous).unwrap();

        let on_disk = std::fs::read_to_string(tmp.path().join("pipelines/one.json")).unwrap();
        assert!(on_disk.contains("a.csv"), "the workspace still holds B: {on_disk}");
        assert!(!on_disk.contains("b.csv"));
        assert!(drift(tmp.path(), &a).is_empty(), "the workspace IS release A again");
    }

    #[test]
    fn a_pipeline_a_release_does_not_contain_is_removed_when_it_is_activated() {
        // Left behind, it is one a schedule can still fire - so the environment
        // would not be running the release.
        let tmp = ws(&[("one.json", ONE)]);
        let a = build(tmp.path()).unwrap();
        save(tmp.path(), &a).unwrap();
        std::fs::write(tmp.path().join("pipelines/later.json"), ONE.replace("one", "later")).unwrap();
        let d = drift(tmp.path(), &a);
        assert_eq!(d.extra, vec!["pipelines/later.json"], "{d:?}");
        materialise(tmp.path(), &a).unwrap();
        assert!(!tmp.path().join("pipelines/later.json").exists());
    }

    #[test]
    fn a_corrupt_object_aborts_before_the_workspace_is_touched() {
        // Every object is read and verified before anything is written, so a
        // failure leaves the workspace as it was rather than half-updated.
        let tmp = ws(&[("one.json", ONE), ("two.json", &ONE.replace("one", "two"))]);
        let r = build(tmp.path()).unwrap();
        save(tmp.path(), &r).unwrap();
        let before = std::fs::read_to_string(tmp.path().join("pipelines/one.json")).unwrap();
        std::fs::write(tmp.path().join("pipelines/one.json"), "{}").unwrap();
        // Corrupt one object.
        let hash = r.body.files.values().next().unwrap().clone();
        std::fs::write(objects_dir(tmp.path()).join(&hash), "tampered").unwrap();
        let e = materialise(tmp.path(), &r).unwrap_err();
        assert!(e.contains("altered"), "{e}");
        assert_eq!(
            std::fs::read_to_string(tmp.path().join("pipelines/one.json")).unwrap(),
            "{}",
            "the workspace was written to despite the failure"
        );
        let _ = before;
    }

    #[test]
    fn objects_are_shared_between_releases() {
        // Two releases differing in one pipeline should cost one extra object,
        // not a second copy of the workspace.
        let tmp = ws(&[("one.json", ONE), ("two.json", &ONE.replace("one", "two"))]);
        build(tmp.path()).unwrap();
        let before = std::fs::read_dir(objects_dir(tmp.path())).unwrap().count();
        std::fs::write(tmp.path().join("pipelines/one.json"), ONE.replace("a.csv", "c.csv")).unwrap();
        build(tmp.path()).unwrap();
        let after = std::fs::read_dir(objects_dir(tmp.path())).unwrap().count();
        assert_eq!(after, before + 1, "unchanged files were stored twice");
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
