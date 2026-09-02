//! #289: named admission pools, so one heavy join does not have to serialise
//! eight cheap HTTP jobs.
//!
//! A single `DUCKLE_MAX_CONCURRENT_RUNS` forces a choice nobody wants to make:
//! set it to 1 and eight lightweight API ingestions run one at a time; set it
//! to 8 and two 24GB joins can start together. Pools let those coexist by
//! naming the kind of work rather than counting runs.
//!
//! ## Admission only
//!
//! A pool answers "may this run start now". What a run may then USE - threads,
//! memory, temp disk - is the existing per-pipeline `resources` block and is
//! untouched. Conflating the two would let a pipeline widen its own memory
//! limit by choosing a different pool.
//!
//! ## One definition, two gates
//!
//! The runner gates with a condvar and the scheduler with a tokio semaphore,
//! because one is sync and the other async. That is fine as long as they agree
//! on the NUMBERS, which is why the numbers live here and not in either of
//! them - two limiters each parsing their own config is how the two schedulers
//! came to disagree about time zones.
//!
//! ## A pipeline may choose a pool, never widen one
//!
//! When a server names the authoritative pools, a workspace can select among
//! them and its own limits are clamped to the server's. Otherwise a pipeline
//! could declare a pool of 999 and opt itself out of the protection pools
//! exist to provide.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// The pool a pipeline gets when it does not ask for one. Sized from
/// `DUCKLE_MAX_CONCURRENT_RUNS`, so a workspace that never mentions pools
/// behaves exactly as it did before.
pub const DEFAULT: &str = "default";

/// What a server or workspace declares for one pool.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct PoolSpec {
    pub max_concurrent_runs: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(transparent)]
pub struct PoolFile {
    pub pools: BTreeMap<String, PoolSpec>,
}

/// The resolved pools for a workspace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pools {
    limits: BTreeMap<String, usize>,
    /// True when a server file decided the set, so an unknown name is a
    /// refusal rather than a new pool.
    authoritative: bool,
}

pub fn env_default() -> usize {
    std::env::var("DUCKLE_MAX_CONCURRENT_RUNS")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(1)
}

pub fn workspace_path(workspace: &Path) -> PathBuf {
    workspace.join(".duckle").join("pools.json")
}

fn read(path: &Path) -> Option<BTreeMap<String, usize>> {
    let text = std::fs::read_to_string(path).ok()?;
    let parsed: PoolFile = serde_json::from_str(&text).ok()?;
    Some(parsed.pools.into_iter().map(|(k, v)| (k, v.max_concurrent_runs.max(1))).collect())
}

impl Pools {
    /// Resolve the pools for a workspace, applying any server ceiling.
    pub fn load(workspace: &Path) -> Pools {
        let server = std::env::var("DUCKLE_POOLS_FILE")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .and_then(|p| read(Path::new(&p)));
        let declared = read(&workspace_path(workspace)).unwrap_or_default();
        let mut limits: BTreeMap<String, usize> = BTreeMap::new();

        match &server {
            // The server decides which pools exist and how big each may be. A
            // workspace may ask for LESS - a team that knows its own jobs are
            // heavy can throttle itself further - and never for more.
            Some(ceiling) => {
                for (name, cap) in ceiling {
                    let asked = declared.get(name).copied().unwrap_or(*cap);
                    limits.insert(name.clone(), asked.min(*cap));
                }
            }
            None => limits.extend(declared),
        }
        limits.entry(DEFAULT.to_string()).or_insert_with(env_default);
        Pools { limits, authoritative: server.is_some() }
    }

    /// Build from explicit numbers, for tests and for callers that already hold
    /// the configuration.
    pub fn from_limits(limits: BTreeMap<String, usize>) -> Pools {
        let mut limits = limits;
        limits.entry(DEFAULT.to_string()).or_insert_with(env_default);
        Pools { limits, authoritative: false }
    }

    /// The pool a request should actually be admitted to.
    ///
    /// A name nobody declared falls back to `default` rather than becoming a
    /// new unbounded pool. That is the point of the server ceiling: a pipeline
    /// naming an undefined pool must not thereby create one.
    pub fn resolve(&self, asked: &str) -> String {
        let asked = asked.trim();
        match !asked.is_empty() && self.limits.contains_key(asked) {
            true => asked.to_string(),
            false => DEFAULT.to_string(),
        }
    }

    pub fn limit(&self, name: &str) -> usize {
        self.limits
            .get(name)
            .copied()
            .unwrap_or_else(|| self.limits.get(DEFAULT).copied().unwrap_or_else(env_default))
    }

    pub fn names(&self) -> impl Iterator<Item = &String> {
        self.limits.keys()
    }

    pub fn is_authoritative(&self) -> bool {
        self.authoritative
    }

    pub fn as_map(&self) -> &BTreeMap<String, usize> {
        &self.limits
    }
}

/// The pool a pipeline document asks for, if any.
///
/// Read off the raw JSON so a document written by an older build, or one
/// carrying the key at the top level as the issue spells it, is understood
/// either way.
pub fn requested(doc: &serde_json::Value) -> String {
    doc.get("resourcePool")
        .and_then(|v| v.as_str())
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ws(body: Option<&str>) -> tempfile::TempDir {
        let tmp = tempfile::tempdir().unwrap();
        if let Some(b) = body {
            std::fs::create_dir_all(tmp.path().join(".duckle")).unwrap();
            std::fs::write(workspace_path(tmp.path()), b).unwrap();
        }
        tmp
    }

    /// `DUCKLE_POOLS_FILE` set for the duration of one closure.
    fn with_server(path: &str, f: impl FnOnce()) {
        std::env::set_var("DUCKLE_POOLS_FILE", path);
        f();
        std::env::remove_var("DUCKLE_POOLS_FILE");
    }

    #[test]
    fn a_workspace_that_never_mentions_pools_behaves_as_before() {
        let tmp = ws(None);
        let p = Pools::load(tmp.path());
        assert_eq!(p.resolve(""), DEFAULT);
        assert_eq!(p.limit(DEFAULT), env_default());
        assert_eq!(p.names().count(), 1);
    }

    #[test]
    fn a_declared_pool_gets_its_own_limit() {
        let tmp = ws(Some(r#"{"heavy":{"maxConcurrentRuns":1},"network":{"maxConcurrentRuns":8}}"#));
        let p = Pools::load(tmp.path());
        assert_eq!(p.limit("heavy"), 1);
        assert_eq!(p.limit("network"), 8);
        assert_eq!(p.resolve("network"), "network");
        assert_eq!(p.resolve("unnamed"), DEFAULT);
    }

    #[test]
    fn an_undeclared_pool_falls_back_rather_than_becoming_unbounded() {
        // A pipeline naming a pool nobody defined must not thereby create one -
        // that would be an opt-out from the protection pools exist to provide.
        let tmp = ws(Some(r#"{"heavy":{"maxConcurrentRuns":1}}"#));
        let p = Pools::load(tmp.path());
        assert_eq!(p.resolve("unlimited"), DEFAULT);
        assert_eq!(p.limit("unlimited"), p.limit(DEFAULT));
    }

    #[test]
    fn a_workspace_may_ask_for_less_than_the_server_allows_and_never_more() {
        let server = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            server.path(),
            r#"{"heavy":{"maxConcurrentRuns":1},"network":{"maxConcurrentRuns":8}}"#,
        )
        .unwrap();
        let tmp = ws(Some(
            r#"{"heavy":{"maxConcurrentRuns":2},"network":{"maxConcurrentRuns":99},
                "mine":{"maxConcurrentRuns":50}}"#,
        ));
        with_server(&server.path().display().to_string(), || {
            let p = Pools::load(tmp.path());
            assert_eq!(p.limit("network"), 8, "a workspace cannot widen a server pool");
            assert_eq!(p.limit("heavy"), 1, "nor exceed it by asking for more");
            assert_eq!(p.resolve("mine"), DEFAULT, "nor invent one the server does not have");
            assert!(p.is_authoritative());
        });
    }

    #[test]
    fn a_workspace_can_throttle_itself_further_than_the_server() {
        let server = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(server.path(), r#"{"network":{"maxConcurrentRuns":8}}"#).unwrap();
        let tmp = ws(Some(r#"{"network":{"maxConcurrentRuns":2}}"#));
        with_server(&server.path().display().to_string(), || {
            assert_eq!(Pools::load(tmp.path()).limit("network"), 2);
        });
    }

    #[test]
    fn a_pool_of_zero_is_one_rather_than_a_deadlock() {
        let tmp = ws(Some(r#"{"stuck":{"maxConcurrentRuns":0}}"#));
        assert_eq!(Pools::load(tmp.path()).limit("stuck"), 1);
    }

    #[test]
    fn an_unreadable_pool_file_leaves_the_default_intact() {
        // Pools are an optimisation; a broken file must not stop a workspace
        // from running anything at all.
        let tmp = ws(Some("{ not json"));
        assert_eq!(Pools::load(tmp.path()).limit(DEFAULT), env_default());
    }

    #[test]
    fn the_requested_pool_is_read_off_the_document() {
        let doc = serde_json::json!({ "resourcePool": " heavy ", "nodes": [] });
        assert_eq!(requested(&doc), "heavy");
        assert_eq!(requested(&serde_json::json!({ "nodes": [] })), "");
    }
}
