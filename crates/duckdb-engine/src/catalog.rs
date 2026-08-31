//! What the whole workspace reads and writes, across every pipeline.
//!
//! Everything Duckle could already tell you about lineage stopped at the edge
//! of one pipeline. `pipeline_impact` inverts column lineage inside a single
//! file; trust, drift and review each take one pipeline and answer about that
//! pipeline. None of them can answer the question an owner of two hundred
//! pipelines actually asks, which is "if I drop this column, or this table
//! moves, what breaks and who do I tell?".
//!
//! This builds the missing half: the graph *between* pipelines. Each source and
//! sink node names something outside the workspace - a file, a table, a topic -
//! and two pipelines that name the same thing are connected whether or not
//! anyone drew a line between them. Collect those names and the connections
//! fall out, along with the things nobody reads and the things nobody writes.
//!
//! # Honesty about coverage
//!
//! An asset name is recovered from a node's properties, and not every connector
//! yields one: a REST source pointed at a templated URL, a component nobody has
//! taught this module about, a node left half-configured. Those are recorded in
//! [`Catalog::unresolved`] rather than dropped. A blast-radius answer that
//! quietly omits the nodes it could not read is worse than no answer, because
//! it looks complete. Anything asking this module a governance question should
//! show that list alongside the result.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Which way data flows between a pipeline and an asset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    Read,
    Write,
}

/// Something outside the workspace that pipelines read or write.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Asset {
    /// Canonical name, stable enough that two pipelines naming the same thing
    /// produce the same string. This is the join key of the whole graph.
    pub id: String,
    /// Broad family: file, table, topic, collection or api.
    pub kind: String,
    /// Columns, as far as the pipelines declare them.
    ///
    /// Taken from the `schema` a node carries rather than by opening the data,
    /// so building the graph still touches no source and needs no credentials.
    /// The union across every node that touches the asset, because one pipeline
    /// may read three columns of a table another writes twenty to, and the
    /// asset has all twenty. Empty means nobody declared any, which is not the
    /// same as the asset having none - the catalog cannot tell those apart and
    /// does not pretend to.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<String>,
}

/// One pipeline touching one asset, at one node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Touch {
    pub pipeline_id: String,
    pub node_id: String,
    pub component_id: String,
    pub asset: String,
    pub direction: Direction,
}

/// A source or sink whose target could not be named, and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Unresolved {
    pub pipeline_id: String,
    pub node_id: String,
    pub component_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PipelineEntry {
    pub id: String,
    pub name: String,
    pub node_count: usize,
}

/// The workspace graph as of the last build.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Catalog {
    pub pipelines: Vec<PipelineEntry>,
    pub assets: Vec<Asset>,
    pub touches: Vec<Touch>,
    /// Nodes this module could not name a target for. Never empty-by-omission:
    /// see the module docs.
    pub unresolved: Vec<Unresolved>,
    /// What the workspace looked like when this was built, so a saved graph can
    /// say whether it still describes the pipelines. Absent on a graph written
    /// before this existed, which is treated as "cannot tell" - see [`is_stale`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub built_from: Option<BuiltFrom>,
}

/// A cheap fingerprint of the pipeline files a graph was built from.
///
/// The catalog is derived, and the console serves the SAVED copy rather than
/// rescanning on every poll - which is right, because rescanning reads every
/// pipeline in the workspace. The cost of that is a graph that can quietly
/// describe pipelines as they were an hour ago, and a blast radius computed
/// from a stale graph is exactly the wrong answer to trust.
///
/// So the build records what it read. Comparing is `stat` only - no file is
/// opened - which is what keeps the check cheap enough to make on every read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BuiltFrom {
    pub files: usize,
    /// Milliseconds since the epoch, of the newest file. Millisecond
    /// resolution because a second is long enough to save two edits in.
    pub newest_mtime: Option<i64>,
    pub total_bytes: u64,
}

/// Fingerprint the workspace's pipeline files as they are right now.
pub fn fingerprint(workspace: &Path) -> BuiltFrom {
    let mut files = 0usize;
    let mut newest: Option<i64> = None;
    let mut total = 0u64;
    for path in discover_pipeline_files(workspace) {
        let Ok(meta) = std::fs::metadata(&path) else { continue };
        files += 1;
        total += meta.len();
        if let Ok(m) = meta.modified() {
            if let Ok(d) = m.duration_since(std::time::UNIX_EPOCH) {
                let ms = d.as_millis() as i64;
                newest = Some(newest.map_or(ms, |n: i64| n.max(ms)));
            }
        }
    }
    BuiltFrom { files, newest_mtime: newest, total_bytes: total }
}

/// Whether a saved graph still describes the workspace.
///
/// A graph with no fingerprint - written before this existed - is reported
/// stale. That is the safe direction: rebuilding an up-to-date graph costs a
/// scan, while serving a stale one costs a wrong answer to "what breaks if I
/// change this".
///
/// The check is deliberately not perfect. Two edits that leave the file count,
/// the total size and the newest timestamp all unchanged would slip through,
/// which needs a same-length edit inside the same millisecond. Catching that
/// would mean hashing every file on every read, and then the check would cost
/// what it exists to avoid.
pub fn is_stale(workspace: &Path, catalog: &Catalog) -> bool {
    match &catalog.built_from {
        None => true,
        Some(built) => *built != fingerprint(workspace),
    }
}

/// Build the graph as it was at a git revision, without touching the worktree.
///
/// This is what the pure builder was split out for. Reviewing a change to a
/// data platform means asking "what does this branch do to the graph" - which
/// asset disappears, which pipeline stops writing the table three others read -
/// and that question cannot be answered by a tool that can only see the files
/// currently on disk.
///
/// Nothing is checked out and nothing is written: the file contents come
/// straight from the object store, so this is safe to run on a dirty worktree
/// and safe to run while somebody else is editing.
pub fn build_at_revision(workspace: &Path, rev: &str) -> Result<Catalog, String> {
    Ok(build_from_documents(&documents_at_revision(workspace, rev)?))
}

/// The pipeline documents as of a git revision (#302).
pub fn documents_at_revision(workspace: &Path, rev: &str) -> Result<Vec<(String, Value)>, String> {
    let listed = git(workspace, &["ls-tree", "-r", "--name-only", rev, "--", "."])?;
    let mut docs: Vec<(String, Value)> = Vec::new();
    for rel in listed.lines().map(str::trim).filter(|l| !l.is_empty()) {
        // Same rules as the on-disk walk, so a revision's graph and the current
        // one are built from the same idea of what a pipeline file is.
        let path = Path::new(rel);
        if path.extension().and_then(|x| x.to_str()) != Some("json") {
            continue;
        }
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if NOT_PIPELINE_FILES.contains(&name) {
            continue;
        }
        if path.components().any(|c| {
            c.as_os_str().to_str().map(|s| NOT_PIPELINES.contains(&s)).unwrap_or(false)
        }) {
            continue;
        }
        // `<rev>:./<path>` resolves relative to this directory, so a workspace
        // that is a subdirectory of the repository works without the caller
        // knowing where the repository root is.
        let Ok(text) = git(workspace, &["show", &format!("{rev}:./{rel}")]) else { continue };
        let Ok(doc): Result<Value, _> = serde_json::from_str(&text) else { continue };
        if doc.get("nodes").and_then(|n| n.as_array()).is_none() {
            continue;
        }
        let id = path.file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
        docs.push((id, doc));
    }
    // No fingerprint is attached by the caller: this describes a revision, not
    // the worktree, so asking whether it is "stale" against the files on disk
    // is meaningless.
    Ok(docs)
}

fn git(workspace: &Path, args: &[&str]) -> Result<String, String> {
    let out = std::process::Command::new("git")
        .arg("-C")
        .arg(workspace)
        .args(args)
        .output()
        .map_err(|e| format!("git: {e}. A revision can only be read from a git workspace."))?;
    if !out.status.success() {
        return Err(format!(
            "git {}: {}",
            args.first().copied().unwrap_or(""),
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).into_owned())
}

/// What changed between two graphs.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogDiff {
    pub assets_added: Vec<String>,
    pub assets_removed: Vec<String>,
    pub pipelines_added: Vec<String>,
    pub pipelines_removed: Vec<String>,
    /// An asset that still exists but has lost every pipeline that wrote it.
    /// The most interesting line in a review: nothing errors, the table simply
    /// stops being updated, and whoever reads it finds out weeks later.
    pub no_longer_written: Vec<String>,
}

impl CatalogDiff {
    pub fn is_empty(&self) -> bool {
        self.assets_added.is_empty()
            && self.assets_removed.is_empty()
            && self.pipelines_added.is_empty()
            && self.pipelines_removed.is_empty()
            && self.no_longer_written.is_empty()
    }
}

/// Compare two graphs, `before` and `after`.
pub fn diff(before: &Catalog, after: &Catalog) -> CatalogDiff {
    let ids = |c: &Catalog| -> std::collections::BTreeSet<String> {
        c.assets.iter().map(|a| a.id.clone()).collect()
    };
    let pipes = |c: &Catalog| -> std::collections::BTreeSet<String> {
        c.pipelines.iter().map(|p| p.id.clone()).collect()
    };
    let (a0, a1) = (ids(before), ids(after));
    let (p0, p1) = (pipes(before), pipes(after));

    // Still there, but nobody writes it any more. This is the change that does
    // not announce itself: no error, no missing file, just a table that quietly
    // stops moving.
    let no_longer_written = a1
        .iter()
        .filter(|id| !before.producers(id).is_empty() && after.producers(id).is_empty())
        .cloned()
        .collect();

    CatalogDiff {
        assets_added: a1.difference(&a0).cloned().collect(),
        assets_removed: a0.difference(&a1).cloned().collect(),
        pipelines_added: p1.difference(&p0).cloned().collect(),
        pipelines_removed: p0.difference(&p1).cloned().collect(),
        no_longer_written,
    }
}

/// An ownership rule that will never fire, and why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeadRule {
    /// "asset" or "pipeline".
    pub kind: String,
    pub pattern: String,
    pub owner: String,
    pub reason: String,
}

/// Ownership rules that match nothing in this workspace.
///
/// A rule that matches nothing fails silently: the team it names simply never
/// gets told about anything, and the file looks correct. Almost always a typo
/// or an asset that was renamed out from under it.
///
/// Lives here rather than in the linter because it has to use the same glob
/// semantics as `for_asset` - a second implementation of "does this match"
/// would eventually disagree with the one that decides who actually owns what.
pub fn dead_rules(catalog: &Catalog, owners: &Owners) -> Vec<DeadRule> {
    let assets: Vec<&str> = catalog.assets.iter().map(|a| a.id.as_str()).collect();
    let pipelines: Vec<&str> = catalog.pipelines.iter().map(|p| p.id.as_str()).collect();
    let mut out = Vec::new();
    for (kind, rules, universe) in [
        ("asset", &owners.assets, &assets),
        ("pipeline", &owners.pipelines, &pipelines),
    ] {
        for rule in rules {
            let reason = match glob::Pattern::new(&rule.pattern) {
                // A pattern that will not compile owns nothing, which is the
                // safe behaviour and an invisible one.
                Err(_) => Some("not a valid glob, so it owns nothing".to_string()),
                Ok(p) if !universe.iter().any(|n| p.matches(n)) => {
                    Some("matches nothing in this workspace".to_string())
                }
                Ok(_) => None,
            };
            if let Some(reason) = reason {
                out.push(DeadRule {
                    kind: kind.to_string(),
                    pattern: rule.pattern.clone(),
                    owner: rule.owner.clone(),
                    reason,
                });
            }
        }
    }
    out
}

/// Everything a catalog screen needs about one workspace, in one call.
///
/// Assembled here rather than in each surface because the desktop app, the web
/// console and MCP all want the same answer, and three assemblies of it would
/// drift into three slightly different catalogs.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogView {
    pub assets: Vec<AssetView>,
    pub pipelines: Vec<PipelineEntry>,
    pub orphans: Vec<String>,
    pub externals: Vec<String>,
    pub unresolved: Vec<Unresolved>,
    /// The workspace glossary, as authored in owners.json.
    pub terms: BTreeMap<String, String>,
    /// True when the pipelines have changed since this graph was built.
    pub stale: bool,
    pub has_owners: bool,
}

/// One asset, with everything known about it joined together.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AssetView {
    pub id: String,
    pub kind: String,
    pub columns: Vec<String>,
    pub written_by: Vec<String>,
    pub read_by: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contact: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub tags: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub freshness: Option<Freshness>,
}

/// Build the whole view: graph, ownership, annotations and freshness.
///
/// Reads the saved graph rather than rebuilding, and reports `stale` instead,
/// so opening a catalog screen never silently costs a full workspace rescan.
pub fn view(workspace: &Path) -> Result<CatalogView, String> {
    let catalog = match load(workspace)? {
        Some(c) => c,
        // Never built: do it once, so the first visit shows something rather
        // than an empty screen that looks like an empty workspace.
        None => build_and_save(workspace)?,
    };
    Ok(view_of(workspace, &catalog))
}

/// The same view, over a graph the caller already has.
///
/// Split from [`view`] so each surface can choose its own freshness policy
/// while they all produce ONE shape. A screen reads the saved graph and shows a
/// stale notice, because rescanning on every poll would read every pipeline in
/// the workspace; an agent asking over MCP builds fresh, because it will not
/// see a notice and cannot press Rescan. Those are different policies about the
/// same answer, which is exactly the thing that must not be duplicated: MCP
/// assembled its own and drifted - `writtenBy` there was a COUNT while the same
/// key on the other two surfaces is a list of pipeline names.
pub fn view_of(workspace: &Path, catalog: &Catalog) -> CatalogView {
    let owners = load_owners(workspace).unwrap_or_default();
    let fresh = freshness(workspace);
    let assets = catalog
        .assets
        .iter()
        .map(|a| {
            let rule = owners.for_asset(&a.id);
            AssetView {
                id: a.id.clone(),
                kind: a.kind.clone(),
                columns: a.columns.clone(),
                written_by: catalog.producers(&a.id).iter().map(|t| t.pipeline_id.clone()).collect(),
                read_by: catalog.consumers(&a.id).iter().map(|t| t.pipeline_id.clone()).collect(),
                owner: rule.map(|r| r.owner.clone()),
                contact: rule.and_then(|r| r.contact.clone()),
                description: rule.and_then(|r| r.description.clone()),
                tags: rule.map(|r| r.tags.clone()).unwrap_or_default(),
                freshness: fresh.get(&a.id).cloned(),
            }
        })
        .collect();
    CatalogView {
        assets,
        pipelines: catalog.pipelines.clone(),
        orphans: catalog.orphans().iter().map(|a| a.id.clone()).collect(),
        externals: catalog.externals().iter().map(|a| a.id.clone()).collect(),
        unresolved: catalog.unresolved.clone(),
        terms: owners.terms.clone(),
        stale: is_stale(workspace, catalog),
        has_owners: !owners.is_empty(),
    }
}

/// Set the human metadata for one exact asset or pipeline name.
///
/// Writes an EXACT-match rule rather than editing whichever glob happened to
/// cover the name. Annotating `/lake/raw/orders.parquet` when the file says
/// `/lake/raw/*` belongs to Data Platform must not silently re-describe every
/// file under `/lake/raw` - so a specific rule goes in ABOVE the general one,
/// which is exactly how first-match-wins is meant to be used. An existing rule
/// whose pattern IS this name is updated in place instead of duplicated.
///
/// Fields left as `None` are left alone, so setting a description does not
/// clear an owner somebody else wrote.
pub fn annotate(
    workspace: &Path,
    pipelines: bool,
    name: &str,
    owner: Option<String>,
    contact: Option<String>,
    description: Option<String>,
    tags: Option<Vec<String>>,
) -> Result<(), String> {
    let name = name.trim();
    if name.is_empty() {
        return Err("annotate needs a name".into());
    }
    let mut owners = load_owners(workspace)?;
    let rules = if pipelines { &mut owners.pipelines } else { &mut owners.assets };

    match rules.iter_mut().position(|r| r.pattern == name) {
        Some(i) => {
            let rule = &mut rules[i];
            if let Some(v) = owner {
                rule.owner = v;
            }
            if contact.is_some() {
                rule.contact = contact;
            }
            if description.is_some() {
                rule.description = description;
            }
            if let Some(v) = tags {
                rule.tags = v;
            }
        }
        None => rules.insert(
            0,
            OwnerRule {
                maximum_age: None,
                pattern: name.to_string(),
                // An annotation with no owner still needs the field; the empty
                // string reads as "not stated" everywhere it is shown.
                owner: owner.unwrap_or_default(),
                contact,
                description,
                tags: tags.unwrap_or_default(),
            },
        ),
    }
    save_owners(workspace, &owners)
}

/// Write owners.json back, preserving everything in it.
///
/// Temp file then rename, like every other store here: this file is
/// hand-authored and committed, and half of it is worse than none of it.
pub fn save_owners(workspace: &Path, owners: &Owners) -> Result<(), String> {
    let p = owners_path(workspace);
    let body = serde_json::to_string_pretty(owners).map_err(|e| e.to_string())?;
    let tmp = p.with_extension(format!("json.{}.tmp", std::process::id()));
    std::fs::write(&tmp, body).map_err(|e| format!("{}: {e}", tmp.display()))?;
    if let Err(e) = std::fs::rename(&tmp, &p) {
        let _ = std::fs::remove_file(&tmp);
        return Err(format!("{}: {e}", p.display()));
    }
    Ok(())
}

/// When an asset was last written, and by what.
///
/// The static graph says an asset exists and who touches it. This says whether
/// it is current, which is the first thing anyone actually asks of a catalog
/// entry - a table nobody has written for three weeks is the interesting one,
/// and no amount of structure reveals that.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Freshness {
    /// RFC3339 of the newest SUCCESSFUL run that wrote it.
    pub last_written_at: String,
    pub pipeline_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows: Option<u64>,
}

/// Freshness for every asset any recorded run has written.
///
/// Reads run history, which is per pipeline, so this is one pass over the runs
/// folder rather than a query per asset.
///
/// Only successful runs count. A failed run may have written nothing, or half
/// of something, and reporting either as "last written" would make a broken
/// load look like a fresh table - the exact reading a freshness column exists
/// to prevent.
pub fn freshness(workspace: &Path) -> BTreeMap<String, Freshness> {
    let mut out: BTreeMap<String, Freshness> = BTreeMap::new();
    let runs = workspace.join("runs");
    let Ok(entries) = std::fs::read_dir(&runs) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|x| x.to_str()) != Some("json") {
            continue;
        }
        let pipeline_id =
            path.file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
        let Ok(text) = std::fs::read_to_string(&path) else { continue };
        let Ok(records) = serde_json::from_str::<Vec<crate::history::RunRecord>>(&text) else {
            continue;
        };
        // #304: a failed run never counted, but an INCOMPLETE one did. An
        // incomplete run stopped at a ceiling, so its rows are correct and are
        // not all of them - treating that as a refresh is the quieter half of
        // "a partial publish must not refresh the asset", because a failure is
        // visible and a truncated success looks healthy.
        for record in records.iter().filter(|r| r.status == "ok" && !r.incomplete) {
            for touch in record.assets.iter().filter(|a| a.direction == "write") {
                let better = match out.get(&touch.id) {
                    // History is appended in order, but two pipelines writing
                    // one asset are two files, so compare rather than assume.
                    Some(existing) => record.at > existing.last_written_at,
                    None => true,
                };
                if better {
                    out.insert(
                        touch.id.clone(),
                        Freshness {
                            last_written_at: record.at.clone(),
                            pipeline_id: pipeline_id.clone(),
                            rows: touch.rows,
                        },
                    );
                }
            }
        }
    }
    out
}

/// The saved graph if it still describes the workspace, else a fresh one.
pub fn load_or_rebuild(workspace: &Path) -> Result<Catalog, String> {
    match load(workspace)? {
        Some(c) if !is_stale(workspace, &c) => Ok(c),
        _ => build_and_save(workspace),
    }
}

/// One ownership rule: a glob over names, and who answers for what it matches.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnerRule {
    /// Glob over asset or pipeline names. `*` matches any characters,
    /// separators included, so `/lake/raw/*` covers everything beneath it.
    #[serde(rename = "match")]
    pub pattern: String,
    pub owner: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contact: Option<String>,
    /// What this data is, in a sentence, for whoever finds it and does not
    /// already know. Carried on the same rule as ownership because they are
    /// authored together and a second file would drift from this one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Free labels: `pii`, `gold`, `deprecated`. Matched exactly, lowercased,
    /// so a catalog can filter by them without inventing a taxonomy nobody
    /// asked for.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    /// #304: how old this asset is allowed to get before it is stale, written
    /// the way an operator writes one - "36h", "2d", "90m".
    ///
    /// On the same rule as ownership for the same reason description and tags
    /// are: they are authored together, and a second file would drift from
    /// this one. A stale asset also needs an owner to tell, which is here.
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "maximumAge")]
    pub maximum_age: Option<String>,
}

/// Who owns what, as authored by a human.
///
/// Rules are globs rather than one entry per asset because a workspace has
/// hundreds of assets and nobody maintains a list that long: ownership is
/// really "this team owns everything under /lake/raw". The first matching rule
/// wins, so put the specific ones above the general ones - the same order a
/// reader assumes when they scan the file top to bottom.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Owners {
    #[serde(default)]
    pub assets: Vec<OwnerRule>,
    #[serde(default)]
    pub pipelines: Vec<OwnerRule>,
    /// Shared vocabulary: term -> what it means here. A workspace where three
    /// teams each define "active customer" differently is the problem a
    /// glossary exists for, and it lives beside ownership because both are the
    /// same kind of hand-authored, reviewable, committed fact.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub terms: BTreeMap<String, String>,
}

impl Owners {
    pub fn for_asset(&self, id: &str) -> Option<&OwnerRule> {
        first_match(&self.assets, id)
    }

    pub fn for_pipeline(&self, id: &str) -> Option<&OwnerRule> {
        first_match(&self.pipelines, id)
    }

    pub fn is_empty(&self) -> bool {
        self.assets.is_empty() && self.pipelines.is_empty() && self.terms.is_empty()
    }
}

fn first_match<'a>(rules: &'a [OwnerRule], name: &str) -> Option<&'a OwnerRule> {
    rules.iter().find(|r| {
        // A pattern that will not compile matches nothing rather than
        // everything: a typo must not silently hand a team the whole workspace.
        glob::Pattern::new(&r.pattern).map(|p| p.matches(name)).unwrap_or(false)
    })
}

/// Authored, so it lives beside the pipelines and belongs in version control -
/// unlike the catalog itself, which is derived and lives under `.duckle`.
pub fn owners_path(workspace: &Path) -> PathBuf {
    workspace.join("owners.json")
}

/// Ownership rules, or an empty set when the workspace has none.
///
/// A file that will not parse is an error rather than "nobody owns anything",
/// because silently reporting every asset as unowned is indistinguishable from
/// the answer a workspace with no file at all should get.
pub fn load_owners(workspace: &Path) -> Result<Owners, String> {
    let p = owners_path(workspace);
    if !p.exists() {
        return Ok(Owners::default());
    }
    let text = std::fs::read_to_string(&p).map_err(|e| e.to_string())?;
    if text.trim().is_empty() {
        return Ok(Owners::default());
    }
    serde_json::from_str(&text).map_err(|e| format!("parse owners.json: {e}"))
}

pub fn catalog_path(workspace: &Path) -> PathBuf {
    workspace.join(".duckle").join("catalog.json")
}

impl Catalog {
    /// Pipelines that write `asset`.
    pub fn producers(&self, asset: &str) -> Vec<&Touch> {
        self.touches
            .iter()
            .filter(|t| t.asset == asset && t.direction == Direction::Write)
            .collect()
    }

    /// Pipelines that read `asset`.
    pub fn consumers(&self, asset: &str) -> Vec<&Touch> {
        self.touches
            .iter()
            .filter(|t| t.asset == asset && t.direction == Direction::Read)
            .collect()
    }

    /// Everything downstream of `asset`: the pipelines that read it, the assets
    /// those pipelines write, the pipelines that read *those*, and so on.
    ///
    /// This is the blast radius of changing or dropping something. The walk
    /// keeps a visited set because workspaces really do contain cycles - a
    /// pipeline that reads a table and writes it back is a normal incremental
    /// pattern - and a cycle must end the walk, not hang it.
    pub fn impact(&self, asset: &str, owners: Option<&Owners>) -> Impact {
        // Index once rather than rescanning the touch list per hop; a workspace
        // with hundreds of pipelines otherwise turns this quadratic.
        let mut reads_by_asset: HashMap<&str, Vec<&Touch>> = HashMap::new();
        let mut writes_by_pipeline: HashMap<&str, Vec<&Touch>> = HashMap::new();
        for t in &self.touches {
            match t.direction {
                Direction::Read => reads_by_asset.entry(&t.asset).or_default().push(t),
                Direction::Write => {
                    writes_by_pipeline.entry(&t.pipeline_id).or_default().push(t)
                }
            }
        }

        let mut seen_assets: HashSet<String> = HashSet::from([asset.to_string()]);
        let mut pipelines: BTreeMap<String, usize> = BTreeMap::new();
        let mut assets: BTreeMap<String, usize> = BTreeMap::new();
        let mut queue: VecDeque<(String, usize)> = VecDeque::from([(asset.to_string(), 0)]);

        while let Some((current, depth)) = queue.pop_front() {
            for read in reads_by_asset.get(current.as_str()).into_iter().flatten() {
                // A pipeline can be reached by several paths; keep the shortest,
                // which is the one a reader will find most believable.
                let entry = pipelines.entry(read.pipeline_id.clone()).or_insert(depth + 1);
                if *entry > depth + 1 {
                    *entry = depth + 1;
                }
                for write in writes_by_pipeline.get(read.pipeline_id.as_str()).into_iter().flatten()
                {
                    if seen_assets.insert(write.asset.clone()) {
                        assets.insert(write.asset.clone(), depth + 1);
                        queue.push_back((write.asset.clone(), depth + 1));
                    }
                }
            }
        }

        Impact {
            asset: asset.to_string(),
            pipelines: pipelines
                .into_iter()
                .map(|(id, depth)| Reached {
                    owner: owners.and_then(|o| o.for_pipeline(&id)).map(|r| r.owner.clone()),
                    id,
                    depth,
                })
                .collect(),
            assets: assets
                .into_iter()
                .map(|(id, depth)| Reached {
                    owner: owners.and_then(|o| o.for_asset(&id)).map(|r| r.owner.clone()),
                    id,
                    depth,
                })
                .collect(),
            unresolved: self.unresolved.len(),
        }
    }

    /// Assets written by some pipeline and read by none. Often a real output,
    /// sometimes a leftover nobody noticed stopped being used.
    pub fn orphans(&self) -> Vec<&Asset> {
        let read: HashSet<&str> = self
            .touches
            .iter()
            .filter(|t| t.direction == Direction::Read)
            .map(|t| t.asset.as_str())
            .collect();
        let written: HashSet<&str> = self
            .touches
            .iter()
            .filter(|t| t.direction == Direction::Write)
            .map(|t| t.asset.as_str())
            .collect();
        self.assets
            .iter()
            .filter(|a| written.contains(a.id.as_str()) && !read.contains(a.id.as_str()))
            .collect()
    }

    /// Assets that no ownership rule matches.
    ///
    /// The useful governance answer is not "here is the owner of this one
    /// thing" but "here are the forty things nobody has claimed", which is the
    /// list that gets worked through before an audit.
    pub fn unowned<'a>(&'a self, owners: &Owners) -> Vec<&'a Asset> {
        self.assets.iter().filter(|a| owners.for_asset(&a.id).is_none()).collect()
    }

    /// Assets read by some pipeline and written by none, so the workspace
    /// depends on them without producing them. These are the external contracts
    /// nobody here controls.
    pub fn externals(&self) -> Vec<&Asset> {
        let written: HashSet<&str> = self
            .touches
            .iter()
            .filter(|t| t.direction == Direction::Write)
            .map(|t| t.asset.as_str())
            .collect();
        let read: HashSet<&str> = self
            .touches
            .iter()
            .filter(|t| t.direction == Direction::Read)
            .map(|t| t.asset.as_str())
            .collect();
        self.assets
            .iter()
            .filter(|a| read.contains(a.id.as_str()) && !written.contains(a.id.as_str()))
            .collect()
    }
}

/// A node reached while walking downstream, and how many hops away it is.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reached {
    pub id: String,
    pub depth: usize,
    /// Who to tell, when the workspace says. `None` means no rule matched,
    /// which is a real answer worth showing rather than a blank.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Impact {
    pub asset: String,
    pub pipelines: Vec<Reached>,
    pub assets: Vec<Reached>,
    /// How many source/sink nodes in the workspace could not be named at all.
    /// Carried on the answer so a caller cannot present it as exhaustive
    /// without also seeing what was missed.
    pub unresolved: usize,
}

/// Folders that hold Duckle's own output rather than pipelines. Kept identical
/// to the console's walk, because a pipeline either of them can open and the
/// other cannot is a hole in whichever answer omits it.
const NOT_PIPELINES: [&str; 8] =
    ["runs", "logs", "connections", "node_modules", ".duckle", ".git", "target", "batches"];

/// Workspace config files that are JSON but are not pipelines.
///
/// They live beside the pipelines and none of them has a `nodes` array, so the
/// builder already skipped them - but the staleness fingerprint stats whatever
/// this walk returns, and with these in it, saving a schedule or writing an
/// owner made the catalog report itself out of date. Nothing about the graph
/// had changed. Named here so both agree on what a pipeline file is.
const NOT_PIPELINE_FILES: [&str; 7] = [
    "owners.json",
    "alerts.json",
    "schedules.json",
    "panel-schedules.json",
    "duckle.json",
    "repository.json",
    "catalog.json",
];

/// Every candidate pipeline file in the workspace.
///
/// This used to read `<workspace>/pipelines/*.json` and nothing else, while the
/// console and the desktop walk the whole workspace and skip the folders above.
/// Both of those support keeping pipelines in subfolders, so a workspace laid
/// out that way had them silently missing from the graph - and a blast radius
/// that quietly omits a pipeline is worse than no answer at all, because it
/// looks like a complete one.
pub fn discover_pipeline_files(workspace: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![workspace.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else { continue };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if !NOT_PIPELINES.contains(&name) {
                    stack.push(path);
                }
            } else if path.extension().and_then(|x| x.to_str()) == Some("json") {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if !NOT_PIPELINE_FILES.contains(&name) {
                    out.push(path);
                }
            }
        }
    }
    // A stable order, so a rebuild that changed nothing produces no diff.
    out.sort();
    out
}

/// The columns a node declares, if any.
///
/// Accepts both shapes the editor has written: objects with a `name`, and bare
/// strings. A node that declares nothing yields nothing, which the catalog
/// reports as "no columns known" rather than as "no columns".
fn declared_columns(node: &Value) -> Vec<String> {
    node.get("data")
        .and_then(|d| d.get("schema"))
        .and_then(|s| s.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|c| {
                    c.get("name").and_then(|n| n.as_str()).or_else(|| c.as_str()).map(String::from)
                })
                .filter(|c: &String| !c.trim().is_empty())
                .collect()
        })
        .unwrap_or_default()
}

/// Read every pipeline in the workspace and build the graph.
/// The pipeline documents currently on disk.
///
/// Split out (#302) so a caller that needs the DOCUMENTS rather than the graph
/// - comparing a revision against the worktree, say - uses the same idea of
/// what a pipeline file is. Two different walks would eventually disagree about
/// which files count, and a contract check that silently skipped a pipeline
/// would report "no breaking changes" for the wrong reason.
pub fn documents(workspace: &Path) -> Vec<(String, Value)> {
    let mut docs: Vec<(String, Value)> = Vec::new();
    for path in discover_pipeline_files(workspace) {
        let Ok(text) = std::fs::read_to_string(&path) else { continue };
        let Ok(doc): Result<Value, _> = serde_json::from_str(&text) else { continue };
        if doc.get("nodes").and_then(|n| n.as_array()).is_none() {
            continue;
        }
        let id = path.file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
        docs.push((id, doc));
    }
    docs
}

pub fn build(workspace: &Path) -> Result<Catalog, String> {
    let docs = documents(workspace);
    let mut catalog = build_from_documents(&docs);
    catalog.built_from = Some(fingerprint(workspace));
    Ok(catalog)
}

/// Build the graph from pipelines already in hand.
///
/// Split from [`build`] so the graph can be derived from documents that are not
/// the ones currently on disk - a git revision, an in-memory edit, a test - and
/// so the derivation itself is testable without a workspace. `build` is now
/// only the disk walk.
pub fn build_from_documents(docs: &[(String, Value)]) -> Catalog {
    let mut catalog = Catalog::default();
    let mut assets: BTreeMap<String, Asset> = BTreeMap::new();

    for (pipeline_id, doc) in docs {
        let Some(nodes) = doc.get("nodes").and_then(|n| n.as_array()) else { continue };
        let pipeline_id = pipeline_id.clone();
        let name = doc
            .get("name")
            .and_then(|n| n.as_str())
            .unwrap_or(&pipeline_id)
            .to_string();
        catalog.pipelines.push(PipelineEntry {
            id: pipeline_id.clone(),
            name,
            node_count: nodes.len(),
        });

        for node in nodes {
            let data = node.get("data").unwrap_or(&Value::Null);
            let component_id = data.get("componentId").and_then(|c| c.as_str()).unwrap_or("");
            let direction = match () {
                _ if component_id.starts_with("src.") => Direction::Read,
                _ if component_id.starts_with("snk.") => Direction::Write,
                // Transforms touch nothing outside the workspace.
                _ => continue,
            };
            let node_id = node.get("id").and_then(|i| i.as_str()).unwrap_or("").to_string();
            let props = data.get("properties").unwrap_or(&Value::Null);

            match asset_of(component_id, props) {
                Ok(asset) => {
                    catalog.touches.push(Touch {
                        pipeline_id: pipeline_id.clone(),
                        node_id,
                        component_id: component_id.to_string(),
                        asset: asset.id.clone(),
                        direction,
                    });
                    // Columns come from what the node declares, so building the
                    // graph still opens no source and needs no credentials.
                    // Unioned across every node touching the asset: a pipeline
                    // reading three columns of a table another writes twenty to
                    // does not make the table three columns wide.
                    let declared = declared_columns(node);
                    let entry = assets.entry(asset.id.clone()).or_insert(asset);
                    for c in declared {
                        if !entry.columns.contains(&c) {
                            entry.columns.push(c);
                        }
                    }
                }
                Err(reason) => catalog.unresolved.push(Unresolved {
                    pipeline_id: pipeline_id.clone(),
                    node_id,
                    component_id: component_id.to_string(),
                    reason,
                }),
            }
        }
    }

    catalog.assets = assets.into_values().collect();
    catalog
}

/// Name the thing a source or sink node points at.
///
/// The rules follow the shapes the connector manifests actually require, most
/// specific first, and were derived by reading the required fields of all 190
/// shipped sources and sinks rather than by guessing. Template placeholders
/// such as `${date}` are deliberately kept: a daily file is one asset with a
/// date in its name, not a new asset every morning, and collapsing them is what
/// makes a dated path joinable across pipelines.
/// An address with the credential taken out of it.
///
/// Asset ids are names, and names get published. `GET /api/catalog` is rated
/// for the **viewer** role, `.duckle/catalog.json` is meant to be committed,
/// and the MCP workspace tools hand out the same strings. Two shipped shapes
/// put a password in the very field that names the server: a `uri` like
/// `mongodb://user:pass@host:27017`, and an ODBC `connectionString` ending
/// `;UID=u;PWD=p`. Neither can be the name.
///
/// Removing it also makes the name *stabler*, which is the whole job of a join
/// key: an id built from a password forks into a second asset the day that
/// password is rotated, and every impact answer spanning the rotation is then
/// wrong in a way nobody would think to check.
///
/// Two shapes are handled, because those are the two the connectors actually
/// produce. A credential passed as a query parameter is not one of them: the
/// `url` branch already drops the query string before calling this, and no
/// shipped connector puts one in an `endpoint`.
fn public_address(raw: &str) -> String {
    // `KEY=value;KEY=value` - an ODBC or JDBC connection string. Segments
    // naming a credential go entirely; what is left still names the server,
    // which is all the graph needs. The scheme and separator checks keep a
    // Hive-style path such as `/lake/dt=2026-08-15;part=1/x.parquet` out of
    // here, since it is a path and not a DSN.
    if !raw.contains("://")
        && raw.contains(';')
        && raw.contains('=')
        && !raw.contains('/')
        && !raw.contains('\\')
    {
        return raw
            .split(';')
            .filter(|seg| !seg.trim().is_empty())
            .filter(|seg| !is_dsn_credential(seg.split('=').next().unwrap_or("").trim()))
            .collect::<Vec<_>>()
            .join(";");
    }
    // `scheme://userinfo@host/tail`, or the same without a scheme. The
    // userinfo is only ever in the authority, so the search stops at the first
    // '/': a path or a query may legitimately contain '@'.
    let (prefix, rest) = match raw.split_once("://") {
        Some((scheme, rest)) => (format!("{scheme}://"), rest),
        None => (String::new(), raw),
    };
    let (authority, tail) = rest.split_at(rest.find('/').unwrap_or(rest.len()));
    let authority = authority.rsplit_once('@').map(|(_, host)| host).unwrap_or(authority);
    format!("{prefix}{authority}{tail}")
}

/// True for a connection-string key that holds a credential.
///
/// ODBC and JDBC spell these much shorter than a Duckle property key does, so
/// the engine's own [`is_secret_prop_key`] does not recognise them. A login
/// name is dropped along with the password on purpose: two pipelines reaching
/// one database under different logins are reading one asset, and keeping the
/// user in the name would split them.
fn is_dsn_credential(key: &str) -> bool {
    let k = key.to_ascii_lowercase();
    matches!(k.as_str(), "pwd" | "uid" | "usr" | "user" | "username")
        || crate::util::is_secret_prop_key(&k)
}

pub fn asset_of(component_id: &str, props: &Value) -> Result<Asset, String> {
    let s = |k: &str| -> Option<String> {
        props
            .get(k)
            .and_then(|v| match v {
                // A port is authored in the GUI as `kind: 'integer'`, so it
                // arrives as a JSON number. Reading only strings dropped it,
                // and two instances on one host then collapsed into one asset:
                // db:5432/sales and db:5433/sales were the same name.
                Value::Number(n) => Some(n.to_string()),
                other => other.as_str().map(str::to_string),
            })
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
    };
    // The connector family, used as the scheme when the target has no natural
    // one: `snk.postgres` -> `postgres`.
    let family = component_id.split('.').nth(1).unwrap_or("duckle");
    // Where the server is, when the properties say. Embedded engines have no
    // authority at all, which is correct: the file is the whole address.
    // A uri-shaped property already names its own scheme, so prefixing the
    // family would give `mongodb://mongodb://...`. Use it as the whole prefix
    // when it does, and build one from the family when it does not.
    let prefixed = |authority: &str| -> String {
        let authority = public_address(authority);
        if authority.contains("://") {
            authority.trim_end_matches('/').to_string()
        } else {
            format!("{family}://{authority}")
        }
    };
    // Join an address to the thing inside it without leaving an empty segment.
    // An absent authority is normal - an embedded database, a SaaS object with
    // no instance named - and `salesforce:///Account` would not join to the
    // same string anyone else produces.
    let addr = |authority: &str, tail: &str| -> String {
        let base = prefixed(authority);
        if base.ends_with("://") {
            format!("{base}{tail}")
        } else {
            format!("{}/{tail}", base.trim_end_matches('/'))
        }
    };
    let authority = || -> String {
        match (s("host").or_else(|| s("endpoint")).or_else(|| s("uri")).or_else(|| s("connectionString")).or_else(|| s("connect")), s("port")) {
            (Some(h), Some(p)) if !h.contains("://") && !h.contains(':') => format!("{h}:{p}"),
            (Some(h), _) => h.trim_end_matches('/').to_string(),
            _ => String::new(),
        }
    };

    // A path already carries its own scheme when it is remote (s3://, gs://,
    // sftp://), so it is used as-is; a local path is normalised to forward
    // slashes so the same file named from Windows and Linux agrees.
    if let Some(path) = s("path") {
        let kind = if path.contains("://") { "object" } else { "file" };
        return Ok(Asset { id: normalise_path(&public_address(&path)), kind: kind.into(), columns: Vec::new() });
    }

    // Object stores that split the address into a bucket and a key.
    if let (Some(bucket), Some(key)) = (s("bucket"), s("key")) {
        return Ok(Asset {
            id: addr(&bucket, key.trim_start_matches('/')),
            kind: "object".into(),
            columns: Vec::new(),
        });
    }

    // Kafka and friends: brokers identify the cluster, topic the stream.
    if let (Some(brokers), Some(topic)) = (s("brokers").or_else(|| s("contactPoints")), s("topic")) {
        return Ok(Asset {
            id: addr(first_host(&brokers), &topic),
            kind: "topic".into(),
            columns: Vec::new(),
        });
    }

    // Search engines address an index on an endpoint.
    if let Some(index) = s("index") {
        return Ok(Asset {
            id: addr(&authority(), &index),
            kind: "index".into(),
            columns: Vec::new(),
        });
    }

    // Document and vector stores: a collection, optionally inside a database.
    if let Some(collection) = s("collection") {
        let db = s("database").map(|d| format!("{d}.")).unwrap_or_default();
        return Ok(Asset {
            id: addr(&authority(), &format!("{db}{collection}")),
            kind: "collection".into(),
            columns: Vec::new(),
        });
    }

    // Relational: a table somewhere. `tableName` is the common spelling and
    // `table` is what the embedded vector stores use.
    if let Some(table) = s("tableName").or_else(|| s("table")) {
        let mut qualified = String::new();
        for part in [s("database"), s("schema")].into_iter().flatten() {
            qualified.push_str(&part);
            qualified.push('.');
        }
        qualified.push_str(&table);
        return Ok(Asset {
            id: addr(&authority(), &qualified),
            kind: "table".into(),
            columns: Vec::new(),
        });
    }

    // SaaS objects, where the object name is the whole target.
    if let Some(object) = s("object").or_else(|| s("objectName")) {
        return Ok(Asset { id: addr(&authority(), &object), kind: "object".into(), columns: Vec::new() });
    }

    // A path on a named server. FTP and SFTP give the host and the remote
    // directory or file in separate required fields, so neither half is the
    // address on its own: naming only the host made every directory on one
    // server the same asset, and joined two pipelines that share nothing but
    // the machine they log in to.
    if let Some(remote) = s("remotePath").or_else(|| s("directory")) {
        return Ok(Asset {
            id: addr(&authority(), remote.trim_start_matches('/')),
            kind: "file".into(),
            columns: Vec::new(),
        });
    }

    // A REST-shaped endpoint. Query strings are dropped: they are usually
    // paging or filter parameters, and keeping them would split one endpoint
    // into an asset per call.
    if let Some(url) = s("url") {
        let without_query = url.split('?').next().unwrap_or(&url);
        return Ok(Asset {
            id: public_address(without_query).trim_end_matches('/').to_string(),
            kind: "api".into(),
            columns: Vec::new(),
        });
    }

    // A whole database, with no finer target named. This is the honest answer
    // for a source that runs its own query: the query text is not an address,
    // and naming the database still links it to everything else on that
    // database, which is what the graph is for.
    if let Some(database) = s("database") {
        return Ok(Asset {
            id: addr(&authority(), &database),
            kind: "database".into(),
            columns: Vec::new(),
        });
    }
    if !authority().is_empty() {
        return Ok(Asset { id: prefixed(&authority()), kind: "database".into(), columns: Vec::new() });
    }
    // Services whose whole address is an account, project or repository, with
    // nothing finer named. Coarse on purpose: naming the service still links
    // every pipeline that uses it, which beats leaving them all unconnected.
    for key in ["account", "project", "workspace", "repo", "indexHost", "contactPoints"] {
        if let Some(v) = s(key) {
            return Ok(Asset {
                id: prefixed(first_host(&v)),
                kind: "service".into(),
            columns: Vec::new(),
            });
        }
    }

    Err(format!(
        "no target property on {component_id}; expected one of path, bucket+key, topic, index, collection, tableName, object, url or database"
    ))
}

/// Lower-case the drive letter and use forward slashes, so `C:\data\x.csv` and
/// `c:/data/x.csv` are recognised as the same file.
fn normalise_path(path: &str) -> String {
    let unified = path.replace('\\', "/");
    let mut chars = unified.chars();
    match (chars.next(), chars.next()) {
        (Some(drive), Some(':')) if drive.is_ascii_alphabetic() => {
            format!("{}:{}", drive.to_ascii_lowercase(), chars.as_str())
        }
        _ => unified,
    }
}

/// The first host in a comma-separated broker list, so `a:9092,b:9092` and
/// `a:9092` name the same cluster.
fn first_host(brokers: &str) -> &str {
    brokers.split(',').next().unwrap_or(brokers).trim()
}

/// Build the graph and persist it, returning what was written.
pub fn build_and_save(workspace: &Path) -> Result<Catalog, String> {
    let catalog = build(workspace)?;
    let p = catalog_path(workspace);
    if let Some(dir) = p.parent() {
        std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    }
    let body = serde_json::to_string_pretty(&catalog).map_err(|e| e.to_string())?;
    // A temp name of this writer's own. One shared `catalog.json.tmp` meant two
    // rebuilds - the console's POST /api/catalog and a `catalog build` in a
    // terminal, which is an ordinary pairing - wrote the same file, so one
    // could rename away the other's half-written bytes. No lock is needed
    // beyond this: both runs derive the same graph from the same pipelines, so
    // a complete last-writer-wins file is the right answer.
    static TMP_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seq = TMP_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let tmp = p.with_extension(format!("json.{}.{seq}.tmp", std::process::id()));
    std::fs::write(&tmp, body).map_err(|e| e.to_string())?;
    // Renamed straight over, with no unlink first: see write_atomically in
    // schedules.rs for why removing the destination is both unnecessary and
    // the thing that opens a window.
    if let Err(e) = std::fs::rename(&tmp, &p) {
        let _ = std::fs::remove_file(&tmp);
        return Err(e.to_string());
    }
    Ok(catalog)
}

/// The last built graph, or None if it has never been built.
pub fn load(workspace: &Path) -> Result<Option<Catalog>, String> {
    let p = catalog_path(workspace);
    if !p.exists() {
        return Ok(None);
    }
    let text = std::fs::read_to_string(&p).map_err(|e| e.to_string())?;
    serde_json::from_str(&text).map(Some).map_err(|e| format!("parse catalog.json: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn write_pipeline(ws: &Path, id: &str, nodes: Value) {
        let dir = ws.join("pipelines");
        std::fs::create_dir_all(&dir).unwrap();
        let doc = json!({ "name": id, "nodes": nodes, "edges": [] });
        std::fs::write(dir.join(format!("{id}.json")), doc.to_string()).unwrap();
    }

    fn node(id: &str, component: &str, props: Value) -> Value {
        json!({ "id": id, "data": { "componentId": component, "properties": props } })
    }

    #[test]
    fn a_table_is_named_the_same_way_from_either_end() {
        // The whole graph hangs on two pipelines naming one thing identically,
        // so a reader and a writer of the same table must agree exactly.
        let reader = asset_of(
            "src.postgres",
            &json!({ "host": "db.internal", "port": "5432", "database": "sales", "schema": "public", "tableName": "orders" }),
        )
        .unwrap();
        let writer = asset_of(
            "snk.postgres",
            &json!({ "host": "db.internal", "port": "5432", "database": "sales", "schema": "public", "tableName": "orders" }),
        )
        .unwrap();
        assert_eq!(reader.id, writer.id);
        assert_eq!(reader.id, "postgres://db.internal:5432/sales.public.orders");
        assert_eq!(reader.kind, "table");
    }

    /// An asset id is a published name, so it must not carry a password.
    ///
    /// `GET /api/catalog` is rated for the viewer role, `.duckle/catalog.json`
    /// is committed, and the MCP tools return the same strings, so a password
    /// spliced into a name reaches all three. Both shipped shapes are covered:
    /// userinfo in a uri, and an ODBC connection string.
    #[test]
    fn an_asset_name_never_carries_the_credential_that_reached_it() {
        // src.mongodb's own placeholder is mongodb://user:pass@host:27017.
        let mongo = asset_of(
            "src.mongodb",
            &json!({ "uri": "mongodb://admin:hunter2@db.internal:27017", "database": "sales", "collection": "orders" }),
        )
        .unwrap();
        assert!(!mongo.id.contains("hunter2"), "the password is in the asset name: {}", mongo.id);
        assert_eq!(mongo.id, "mongodb://db.internal:27017/sales.orders");

        // And the same server named without a credential is the SAME asset,
        // which is the point: the name is a join key, so it cannot depend on
        // who connected or on a password that will be rotated.
        let plain = asset_of(
            "snk.mongodb",
            &json!({ "uri": "mongodb://db.internal:27017", "database": "sales", "collection": "orders" }),
        )
        .unwrap();
        assert_eq!(mongo.id, plain.id, "rotating the password forked one asset into two");

        // src.teradata's own placeholder ends ...;UID=...;PWD=...
        let odbc = asset_of(
            "src.teradata",
            &json!({ "connectionString": "DRIVER={Teradata Database ODBC Driver 17.20};DBCNAME=td.internal;UID=etl;PWD=hunter2", "database": "sales", "tableName": "orders" }),
        )
        .unwrap();
        assert!(!odbc.id.contains("hunter2"), "the password is in the asset name: {}", odbc.id);
        assert!(!odbc.id.contains("UID=etl"), "the login is in the asset name: {}", odbc.id);
        assert!(odbc.id.contains("DBCNAME=td.internal"), "the server was lost: {}", odbc.id);

        // A REST endpoint reached with basic credentials in the URL.
        let api = asset_of("src.rest", &json!({ "url": "https://svc:tok3n@api.example.com/v1/orders" })).unwrap();
        assert_eq!(api.id, "https://api.example.com/v1/orders");

        // And an sftp path, where the same shape appears in `path`.
        let sftp = asset_of("src.xml", &json!({ "path": "sftp://etl:hunter2@files.internal/in/orders.xml" })).unwrap();
        assert_eq!(sftp.id, "sftp://files.internal/in/orders.xml");
    }

    /// Saving a schedule must not make the catalog look out of date.
    ///
    /// The staleness fingerprint stats whatever the pipeline walk returns, and
    /// the walk took every .json in the workspace - so writing owners.json,
    /// alerts.json or schedules.json flipped the graph to stale even though
    /// nothing the graph is built from had changed. A catalog that cries stale
    /// on every unrelated save is one nobody reads the warning on.
    #[test]
    fn writing_workspace_config_does_not_make_the_graph_stale() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        write_pipeline(ws, "load", json!([node("k", "snk.parquet", json!({ "path": "/lake/a.parquet" }))]));
        let built = build_and_save(ws).unwrap();
        assert!(!is_stale(ws, &built));

        for name in ["owners.json", "alerts.json", "schedules.json", "repository.json"] {
            std::fs::write(ws.join(name), r#"{"written":"just now"}"#).unwrap();
            assert!(!is_stale(ws, &built), "writing {name} made the graph report itself stale");
        }
        // A real pipeline still does.
        write_pipeline(ws, "second", json!([node("s", "src.parquet", json!({ "path": "/lake/a.parquet" }))]));
        assert!(is_stale(ws, &built), "adding a pipeline no longer registers");
    }

    /// The change that does not announce itself.
    ///
    /// An asset that disappears is loud - something errors. An asset that is
    /// still there but has lost every pipeline that WROTE it is silent: no
    /// error, no missing file, the table just stops moving and whoever reads it
    /// finds out weeks later. That is the line a review needs most.
    #[test]
    fn a_diff_names_the_asset_nothing_writes_any_more() {
        let writer = |path: &str| {
            json!({ "name": "writer", "nodes": [node("k", "snk.parquet", json!({ "path": path }))], "edges": [] })
        };
        let reader = json!({ "name": "reader", "nodes": [node("s", "src.parquet", json!({ "path": "/lake/orders.parquet" }))], "edges": [] });
        let legacy = json!({ "name": "legacy", "nodes": [node("k", "snk.parquet", json!({ "path": "/lake/legacy.parquet" }))], "edges": [] });

        let before = build_from_documents(&[
            ("writer".into(), writer("/lake/orders.parquet")),
            ("reader".into(), reader.clone()),
            ("legacy".into(), legacy),
        ]);
        // The change: legacy deleted, and writer now writes somewhere else.
        let after = build_from_documents(&[
            ("writer".into(), writer("/lake/orders_v2.parquet")),
            ("reader".into(), reader),
        ]);

        let d = diff(&before, &after);
        assert_eq!(d.pipelines_removed, vec!["legacy"]);
        assert_eq!(d.assets_added, vec!["/lake/orders_v2.parquet"]);
        assert_eq!(d.assets_removed, vec!["/lake/legacy.parquet"]);
        // The quiet one: still named by the reader, but nothing writes it.
        assert_eq!(d.no_longer_written, vec!["/lake/orders.parquet"]);
        assert!(!d.is_empty());

        // Comparing a graph with itself reports nothing, or every review would
        // be noise and the real findings would be scrolled past.
        assert!(diff(&after, &after).is_empty());
    }

    /// A graph can be built from a git revision without touching the worktree.
    #[test]
    fn a_revision_is_read_from_git_not_from_the_files_on_disk() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let git = |args: &[&str]| {
            std::process::Command::new("git").arg("-C").arg(ws).args(args).output()
        };
        // No git, no test - reported rather than silently passing.
        if git(&["init", "-q", "."]).map(|o| !o.status.success()).unwrap_or(true) {
            eprintln!("skipping: git is not available");
            return;
        }
        let _ = git(&["config", "user.email", "t@t"]);
        let _ = git(&["config", "user.name", "t"]);

        write_pipeline(ws, "load", json!([node("k", "snk.parquet", json!({ "path": "/lake/committed.parquet" }))]));
        let _ = git(&["add", "-A"]);
        let _ = git(&["commit", "-qm", "base"]);

        // Now change the worktree WITHOUT committing.
        write_pipeline(ws, "load", json!([node("k", "snk.parquet", json!({ "path": "/lake/uncommitted.parquet" }))]));

        let at_head = build_at_revision(ws, "HEAD").expect("could not read the revision");
        let ids: Vec<&str> = at_head.assets.iter().map(|a| a.id.as_str()).collect();
        assert_eq!(ids, vec!["/lake/committed.parquet"], "it read the worktree, not the revision");

        // ...and the worktree still says what it says, untouched.
        let now = build(ws).unwrap();
        assert_eq!(now.assets[0].id, "/lake/uncommitted.parquet");

        // A revision describes a revision, so asking whether it is stale
        // against the files on disk is meaningless and it records no
        // fingerprint at all.
        assert!(at_head.built_from.is_none());
    }

    /// A rule that matches nothing fails silently, so it has to be reported.
    ///
    /// The team a typo'd rule names simply never gets told about anything, and
    /// the file looks perfectly correct. Same for a pattern that will not
    /// compile: it owns nothing, safely and invisibly.
    #[test]
    fn ownership_rules_that_can_never_fire_are_reported() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        write_pipeline(ws, "load", json!([node("k", "snk.parquet", json!({ "path": "/lake/orders.parquet" }))]));
        let cat = build(ws).unwrap();

        let owners: Owners = serde_json::from_str(
            r#"{"assets":[
                 {"match":"/lake/orders.parquet","owner":"Ingest"},
                 {"match":"/lake/odrers.parquet","owner":"Typo Team"},
                 {"match":"/lake/[unclosed","owner":"Broken Glob"}
               ],
               "pipelines":[{"match":"load","owner":"Ingest"},
                            {"match":"never-built-*","owner":"Ghost"}]}"#,
        )
        .unwrap();

        let dead = dead_rules(&cat, &owners);
        let patterns: Vec<&str> = dead.iter().map(|d| d.pattern.as_str()).collect();
        assert_eq!(patterns, vec!["/lake/odrers.parquet", "/lake/[unclosed", "never-built-*"]);
        // The two failures are told apart, because they call for different fixes.
        assert!(dead[0].reason.contains("matches nothing"));
        assert!(dead[1].reason.contains("not a valid glob"));
        assert_eq!(dead[2].kind, "pipeline");

        // A rule that DOES match is never reported, or the check is noise.
        assert!(!patterns.contains(&"/lake/orders.parquet"));
        assert!(!patterns.contains(&"load"));
    }

    /// Annotating one asset must not re-describe everything a glob covers.
    ///
    /// owners.json is glob-based and first-match-wins. If describing
    /// /lake/raw/orders.parquet edited whichever rule happened to match it,
    /// a workspace with `/lake/raw/*` owned by Data Platform would silently
    /// have every file under /lake/raw re-described - and the person who did it
    /// would have no idea. So a specific rule goes in ABOVE the general one,
    /// which is exactly what first-match-wins is for.
    #[test]
    fn annotating_one_asset_does_not_rewrite_the_glob_that_covered_it() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        std::fs::write(
            owners_path(ws),
            r#"{"assets":[{"match":"/lake/raw/*","owner":"Data Platform","contact":"dp@acme.test"}]}"#,
        )
        .unwrap();

        annotate(
            ws,
            false,
            "/lake/raw/orders.parquet",
            None,
            None,
            Some("Orders, one row per line item.".into()),
            Some(vec!["gold".into()]),
        )
        .unwrap();

        let owners = load_owners(ws).unwrap();
        assert_eq!(owners.assets.len(), 2, "the broad rule was edited instead of a specific one added");
        assert_eq!(owners.assets[0].pattern, "/lake/raw/orders.parquet", "the specific rule must come first");

        // The one asset gets the description...
        let orders = owners.for_asset("/lake/raw/orders.parquet").unwrap();
        assert_eq!(orders.description.as_deref(), Some("Orders, one row per line item."));
        assert_eq!(orders.tags, vec!["gold"]);
        // ...and its neighbour under the same glob is untouched.
        let other = owners.for_asset("/lake/raw/customers.parquet").unwrap();
        assert_eq!(other.owner, "Data Platform");
        assert!(other.description.is_none(), "a sibling was re-described");
    }

    /// Annotating twice edits the rule rather than stacking duplicates, and a
    /// field left unset is left alone.
    #[test]
    fn a_second_annotation_updates_in_place_and_keeps_what_it_was_not_given() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        annotate(ws, false, "/lake/orders.parquet", Some("Ingest".into()), None, Some("First".into()), None)
            .unwrap();
        annotate(ws, false, "/lake/orders.parquet", None, None, Some("Second".into()), None).unwrap();

        let owners = load_owners(ws).unwrap();
        assert_eq!(owners.assets.len(), 1, "a second annotation added a duplicate rule");
        let rule = &owners.assets[0];
        assert_eq!(rule.description.as_deref(), Some("Second"));
        assert_eq!(rule.owner, "Ingest", "writing a description cleared the owner");
    }

    /// The view joins the graph, ownership, annotations and freshness.
    #[test]
    fn the_view_carries_everything_a_catalog_screen_needs() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        write_pipeline(ws, "load", json!([node("k", "snk.parquet", json!({ "path": "/lake/orders.parquet" }))]));
        build_and_save(ws).unwrap();
        annotate(ws, false, "/lake/orders.parquet", Some("Ingest".into()), None, Some("Orders.".into()), Some(vec!["gold".into()])).unwrap();

        let v = view(ws).unwrap();
        assert_eq!(v.assets.len(), 1);
        let a = &v.assets[0];
        assert_eq!(a.owner.as_deref(), Some("Ingest"));
        assert_eq!(a.description.as_deref(), Some("Orders."));
        assert_eq!(a.tags, vec!["gold"]);
        assert_eq!(a.written_by, vec!["load"]);
        // Written by nobody yet, so no freshness rather than a fabricated one.
        assert!(a.freshness.is_none());
        assert!(!v.stale, "a freshly built graph reported itself stale");
        assert!(v.has_owners);
    }

    /// The catalog can say when an asset was last written, and by what.
    ///
    /// A static graph says an asset exists and who touches it. Freshness is the
    /// first thing anyone actually asks of a catalog entry - a table nobody has
    /// written for three weeks is the interesting one, and no amount of
    /// structure reveals that.
    #[test]
    fn an_asset_reports_when_it_was_last_written_and_by_which_pipeline() {
        use crate::history::{AssetTouch, RunRecord};
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        std::fs::create_dir_all(ws.join("runs")).unwrap();

        let record = |at: &str, status: &str, asset: &str, dir: &str, rows: Option<u64>| RunRecord {
            at: at.into(),
            status: status.into(),
            duration_ms: 10,
            rows: rows.unwrap_or(0),
            node_count: 1,
            trigger: "scheduled".into(),
            error: None,
            category: None,
            incomplete: false,
            incomplete_reason: None,
            assets: vec![AssetTouch { id: asset.into(), direction: dir.into(), rows }],
            run_id: None,
            unchanged: false,
        };

        std::fs::write(
            ws.join("runs").join("nightly-load.json"),
            serde_json::to_string(&vec![
                record("2026-08-01T02:00:00Z", "ok", "/lake/orders.parquet", "write", Some(1_000)),
                record("2026-08-16T02:00:00Z", "ok", "/lake/orders.parquet", "write", Some(4_100_000)),
                // A LATER failure must not present itself as the last write: a
                // failed run may have written nothing, or half of something.
                record("2026-08-16T03:00:00Z", "error", "/lake/orders.parquet", "write", None),
            ])
            .unwrap(),
        )
        .unwrap();
        // A reader of the same asset says nothing about its freshness.
        std::fs::write(
            ws.join("runs").join("report.json"),
            serde_json::to_string(&vec![record(
                "2026-08-16T09:00:00Z",
                "ok",
                "/lake/orders.parquet",
                "read",
                Some(4_100_000),
            )])
            .unwrap(),
        )
        .unwrap();

        let fresh = freshness(ws);
        let orders = fresh.get("/lake/orders.parquet").expect("no freshness for a written asset");
        assert_eq!(orders.last_written_at, "2026-08-16T02:00:00Z", "a failed run was taken as the last write");
        assert_eq!(orders.pipeline_id, "nightly-load");
        assert_eq!(orders.rows, Some(4_100_000));
        assert_eq!(fresh.len(), 1, "a read was counted as a write");
    }

    /// Run records written before assets existed still parse.
    #[test]
    fn an_old_run_record_without_assets_still_loads() {
        let old = r#"[{"at":"2026-08-01T02:00:00Z","status":"ok","duration_ms":10,
                       "rows":5,"node_count":2,"trigger":"manual"}]"#;
        let records: Vec<crate::history::RunRecord> = serde_json::from_str(old).unwrap();
        assert_eq!(records.len(), 1);
        assert!(records[0].assets.is_empty());
    }

    /// A saved graph knows whether it still describes the workspace.
    ///
    /// The console serves the saved copy rather than rescanning on every poll,
    /// which is right - rescanning reads every pipeline - but it means the
    /// graph can quietly describe pipelines as they were an hour ago. A blast
    /// radius computed from a stale graph is precisely the wrong answer to
    /// trust, so the graph records what it was built from.
    #[test]
    fn a_saved_graph_notices_when_the_pipelines_have_moved_on() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        write_pipeline(ws, "one", json!([node("k", "snk.parquet", json!({ "path": "/lake/a.parquet" }))]));

        let built = build_and_save(ws).unwrap();
        assert!(built.built_from.is_some(), "the build recorded nothing about its inputs");
        assert!(!is_stale(ws, &built), "a graph is stale the moment it is built");

        // A new pipeline: the file count moves.
        write_pipeline(ws, "two", json!([node("s", "src.parquet", json!({ "path": "/lake/a.parquet" }))]));
        assert!(is_stale(ws, &built), "adding a pipeline did not make the graph stale");

        // Rebuilding settles it, and picks up the new pipeline.
        let fresh = load_or_rebuild(ws).unwrap();
        assert!(!is_stale(ws, &fresh));
        assert_eq!(fresh.pipelines.len(), 2);

        // An edit that changes the file's size is caught too.
        write_pipeline(
            ws,
            "two",
            json!([node("s", "src.parquet", json!({ "path": "/lake/somewhere/else/entirely.parquet" }))]),
        );
        assert!(is_stale(ws, &fresh), "editing a pipeline did not make the graph stale");
    }

    /// A graph written before fingerprints existed is treated as stale.
    ///
    /// The safe direction: rebuilding an up-to-date graph costs a scan, while
    /// serving a stale one costs a wrong answer to a governance question.
    #[test]
    fn a_graph_that_cannot_say_what_it_was_built_from_is_not_trusted() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        write_pipeline(ws, "one", json!([node("k", "snk.parquet", json!({ "path": "/lake/a.parquet" }))]));
        let mut old = build_and_save(ws).unwrap();
        old.built_from = None;
        assert!(is_stale(ws, &old), "a graph with no fingerprint was trusted");

        // ...and load_or_rebuild replaces it rather than serving it.
        std::fs::write(catalog_path(ws), serde_json::to_string(&old).unwrap()).unwrap();
        let got = load_or_rebuild(ws).unwrap();
        assert!(got.built_from.is_some());
    }

    /// The graph can be derived from documents, not only from disk.
    ///
    /// Splitting the walk from the derivation is what lets the same graph be
    /// built for a git revision, an unsaved edit, or a test - and it is what
    /// makes the derivation testable at all without laying out a workspace.
    #[test]
    fn a_graph_can_be_built_from_documents_in_hand() {
        let docs = vec![
            (
                "writer".to_string(),
                json!({ "name": "writer", "nodes": [
                    node("k", "snk.parquet", json!({ "path": "/lake/orders.parquet" }))
                ], "edges": [] }),
            ),
            (
                "reader".to_string(),
                json!({ "name": "reader", "nodes": [
                    node("s", "src.parquet", json!({ "path": "/lake/orders.parquet" }))
                ], "edges": [] }),
            ),
        ];
        let cat = build_from_documents(&docs);
        assert_eq!(cat.pipelines.len(), 2);
        assert_eq!(cat.assets.len(), 1, "the two pipelines did not join on one asset");
        let wrote: Vec<&str> = cat.producers("/lake/orders.parquet").iter().map(|t| t.pipeline_id.as_str()).collect();
        let read: Vec<&str> = cat.consumers("/lake/orders.parquet").iter().map(|t| t.pipeline_id.as_str()).collect();
        assert_eq!(wrote, vec!["writer"]);
        assert_eq!(read, vec!["reader"]);
    }

    /// An asset carries the columns the pipelines declare, unioned.
    ///
    /// A pipeline reading three columns of a table another writes twenty to
    /// does not make the table three columns wide, so the union is the only
    /// honest answer. Read from the node's declared schema, so building the
    /// graph still opens no source and needs no credentials.
    #[test]
    fn an_asset_gathers_the_columns_every_pipeline_declares() {
        let with_schema = |id: &str, comp: &str, cols: Value| {
            let mut n = node(id, comp, json!({ "path": "/lake/orders.parquet" }));
            n["data"]["schema"] = cols;
            n
        };
        let docs = vec![
            (
                "writer".to_string(),
                json!({ "name": "w", "nodes": [with_schema("k", "snk.parquet",
                    json!([{"name":"id"},{"name":"total"},{"name":"placed_at"}]))], "edges": [] }),
            ),
            (
                "reader".to_string(),
                // Reads two, one of which the writer never declared, and uses
                // the bare-string schema shape the editor also writes.
                json!({ "name": "r", "nodes": [with_schema("s", "src.parquet",
                    json!(["id", "currency"]))], "edges": [] }),
            ),
        ];
        let cat = build_from_documents(&docs);
        let asset = &cat.assets[0];
        assert_eq!(asset.columns, vec!["id", "total", "placed_at", "currency"]);

        // A workspace that declares nothing reports nothing, rather than
        // claiming the asset has no columns.
        let bare = build_from_documents(&[(
            "p".to_string(),
            json!({ "name": "p", "nodes": [node("k", "snk.parquet", json!({ "path": "/x.parquet" }))], "edges": [] }),
        )]);
        assert!(bare.assets[0].columns.is_empty());
    }

    /// owners.json carries the human half: description, tags and a glossary.
    #[test]
    fn owners_json_also_holds_descriptions_tags_and_a_glossary() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        std::fs::write(
            owners_path(ws),
            r#"{
              "assets": [
                {"match": "/lake/raw/*", "owner": "Data Platform",
                 "contact": "dp@acme.test",
                 "description": "Raw landing zone, one file per source table.",
                 "tags": ["raw", "pii"]}
              ],
              "terms": {"active customer": "Ordered in the last 90 days."}
            }"#,
        )
        .unwrap();

        let owners = load_owners(ws).unwrap();
        let rule = owners.for_asset("/lake/raw/orders.parquet").expect("no rule matched");
        assert_eq!(rule.owner, "Data Platform");
        assert_eq!(rule.description.as_deref(), Some("Raw landing zone, one file per source table."));
        assert_eq!(rule.tags, vec!["raw", "pii"]);
        assert_eq!(owners.terms["active customer"], "Ordered in the last 90 days.");

        // An owners.json written before any of this still loads: every new
        // field is optional, so upgrading does not invalidate the file people
        // already committed.
        std::fs::write(owners_path(ws), r#"{"assets":[{"match":"*","owner":"Someone"}]}"#).unwrap();
        let old = load_owners(ws).unwrap();
        assert_eq!(old.for_asset("anything").unwrap().owner, "Someone");
        assert!(old.terms.is_empty());
    }

    /// A pipeline the console can open must be a pipeline the graph can see.
    ///
    /// The catalog read `<workspace>/pipelines/*.json` and nothing else, while
    /// the console and the desktop walk the workspace. A pipeline in a
    /// subfolder was therefore missing from every impact answer, silently.
    #[test]
    fn pipelines_in_subfolders_are_in_the_graph_and_duckle_s_own_folders_are_not() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();

        // The layout the flat scan saw.
        write_pipeline(ws, "flat", json!([node("a", "snk.parquet", json!({ "path": "/lake/flat.parquet" }))]));

        // A pipeline organised into a subfolder, which both editors support.
        let nested = ws.join("pipelines").join("nightly");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(
            nested.join("load.json"),
            json!({ "name": "load", "nodes": [node("a", "src.parquet", json!({ "path": "/lake/flat.parquet" }))], "edges": [] }).to_string(),
        )
        .unwrap();

        // And one outside the pipelines folder entirely.
        let other = ws.join("flows");
        std::fs::create_dir_all(&other).unwrap();
        std::fs::write(
            other.join("export.json"),
            json!({ "name": "export", "nodes": [node("a", "src.parquet", json!({ "path": "/lake/flat.parquet" }))], "edges": [] }).to_string(),
        )
        .unwrap();

        // Duckle's own output must not be mistaken for pipelines. A run record
        // has no nodes array, but .duckle holds documents that do.
        let hidden = ws.join(".duckle");
        std::fs::create_dir_all(&hidden).unwrap();
        std::fs::write(
            hidden.join("catalog.json"),
            json!({ "name": "not-a-pipeline", "nodes": [node("a", "snk.parquet", json!({ "path": "/lake/should-not-appear.parquet" }))], "edges": [] }).to_string(),
        )
        .unwrap();

        let cat = build(ws).unwrap();
        let ids: Vec<&str> = cat.pipelines.iter().map(|p| p.id.as_str()).collect();
        assert!(ids.contains(&"flat"), "the flat pipeline was lost: {ids:?}");
        assert!(ids.contains(&"load"), "a pipeline in a subfolder is missing from the graph: {ids:?}");
        assert!(ids.contains(&"export"), "a pipeline outside pipelines/ is missing: {ids:?}");
        assert!(!ids.contains(&"not-a-pipeline"), "walked into .duckle: {ids:?}");

        // And the point of finding them: they join.
        let hit = cat.impact("/lake/flat.parquet", None);
        let reached: Vec<&str> = hit.pipelines.iter().map(|p| p.id.as_str()).collect();
        assert!(
            reached.contains(&"load") && reached.contains(&"export"),
            "the blast radius omitted a pipeline that reads the asset: {reached:?}"
        );
    }

    /// Two directories on one FTP server are two assets.
    #[test]
    fn an_ftp_path_is_named_not_just_the_server_it_sits_on() {
        // src.ftp requires host + directory; snk.ftp requires host + remotePath.
        // Naming only the host made every path on one server one asset, so two
        // unrelated pipelines looked connected.
        let inbox = asset_of("src.ftp", &json!({ "host": "files.internal", "directory": "/in/orders" })).unwrap();
        let archive = asset_of("src.ftp", &json!({ "host": "files.internal", "directory": "/in/archive" })).unwrap();
        assert_ne!(inbox.id, archive.id, "two directories on one server were named as one asset");
        assert_eq!(inbox.id, "ftp://files.internal/in/orders");
        assert_eq!(inbox.kind, "file");

        // And a sink writing where a source reads is still the same asset.
        let written = asset_of("snk.ftp", &json!({ "host": "files.internal", "remotePath": "/in/orders" })).unwrap();
        assert_eq!(inbox.id, written.id, "a reader and a writer of one path disagree");
    }

    /// A path is not a connection string, however many '=' it contains.
    #[test]
    fn a_partitioned_path_is_left_alone() {
        // Hive-style partition names are '=' separated and a path may contain
        // ';'. Mistaking one for a DSN would rewrite the name of a real file.
        let a = asset_of("src.parquet", &json!({ "path": "/lake/dt=2026-08-15;run=1/orders.parquet" })).unwrap();
        assert_eq!(a.id, "/lake/dt=2026-08-15;run=1/orders.parquet");
    }

    /// The GUI writes a port as a number, and a number is still a port.
    #[test]
    fn two_instances_on_one_host_are_two_assets_even_when_the_port_is_a_number() {
        // manifest-synth declares port as `kind: 'integer'`, so every
        // GUI-authored node carries a JSON number here. Reading only strings
        // dropped it, and both of these collapsed onto postgres://db/sales...
        let a = asset_of(
            "src.postgres",
            &json!({ "host": "db", "port": 5432, "database": "sales", "schema": "public", "tableName": "orders" }),
        )
        .unwrap();
        let b = asset_of(
            "src.postgres",
            &json!({ "host": "db", "port": 5433, "database": "sales", "schema": "public", "tableName": "orders" }),
        )
        .unwrap();
        assert_eq!(a.id, "postgres://db:5432/sales.public.orders");
        assert_ne!(a.id, b.id, "two instances on one host were named as one asset");

        // A hand-written string port must still name the same asset as the
        // number the GUI writes, or the two authoring paths would disagree.
        let text = asset_of(
            "src.postgres",
            &json!({ "host": "db", "port": "5432", "database": "sales", "schema": "public", "tableName": "orders" }),
        )
        .unwrap();
        assert_eq!(a.id, text.id);
    }

    #[test]
    fn the_same_file_written_two_ways_is_one_asset() {
        let windows = asset_of("snk.parquet", &json!({ "path": "C:\\data\\orders.parquet" })).unwrap();
        let posix = asset_of("src.parquet", &json!({ "path": "c:/data/orders.parquet" })).unwrap();
        assert_eq!(windows.id, posix.id, "one file was counted as two assets");

        // A remote path keeps its scheme and is not a local file.
        let remote = asset_of("snk.s3", &json!({ "path": "s3://bucket/curated/orders.parquet" })).unwrap();
        assert_eq!(remote.kind, "object");
        assert_eq!(remote.id, "s3://bucket/curated/orders.parquet");
    }

    #[test]
    fn a_dated_path_stays_one_asset_rather_than_one_per_day() {
        // Collapsing the template is the point: otherwise a daily export looks
        // like a new, unread asset every morning and impact never finds it.
        let a = asset_of("snk.csv", &json!({ "path": "/exports/orders_${date}.csv" })).unwrap();
        let b = asset_of("src.csv", &json!({ "path": "/exports/orders_${date}.csv" })).unwrap();
        assert_eq!(a.id, b.id);
        assert!(a.id.contains("${date}"), "the placeholder was expanded away");
    }

    #[test]
    fn streams_and_collections_and_endpoints_are_named() {
        let topic = asset_of("src.kafka", &json!({ "brokers": "a:9092,b:9092", "topic": "orders" })).unwrap();
        assert_eq!(topic.id, "kafka://a:9092/orders");
        assert_eq!(topic.kind, "topic");

        let coll = asset_of(
            "snk.mongodb",
            &json!({ "uri": "mongodb://m:27017/", "database": "sales", "collection": "orders" }),
        )
        .unwrap();
        assert_eq!(coll.id, "mongodb://m:27017/sales.orders");

        // Paging parameters must not split one endpoint into many assets.
        let api = asset_of("src.rest", &json!({ "url": "https://api.example.com/v1/orders?page=3" })).unwrap();
        assert_eq!(api.id, "https://api.example.com/v1/orders");
    }

    /// One case per connector family, using the property sets those families
    /// actually mark required in their shipped manifests.
    ///
    /// Measured against all 190 shipped sources and sinks by their required
    /// fields alone, these rules name 184. The six they miss are `src.clipboard`
    /// and `src.webhook`, which have no external target to name at all;
    /// `src.adbc` and `src.salesforce.bulk`, which are given a raw query rather
    /// than an address; and `src.duckdb` and `src.teradata`, which require
    /// nothing and are named at run time from whichever optional database or
    /// table is actually set. That last pair is why 184 is a floor rather than
    /// the real figure: this counts only what a manifest guarantees is present.
    #[test]
    fn every_connector_family_yields_a_name() {
        let cases: Vec<(&str, Value, &str)> = vec![
            ("src.csv", json!({ "path": "/in/a.csv" }), "file"),
            ("snk.gcs", json!({ "bucket": "warehouse", "key": "/curated/a.parquet" }), "object"),
            ("src.kafka", json!({ "brokers": "a:9092", "topic": "orders" }), "topic"),
            ("src.elastic", json!({ "endpoint": "http://es:9200", "index": "orders" }), "index"),
            ("src.qdrant", json!({ "collection": "embeddings" }), "collection"),
            ("snk.postgres", json!({ "host": "db", "database": "sales", "tableName": "orders" }), "table"),
            ("snk.lancedb", json!({ "uri": "/lake/lance", "table": "vectors" }), "table"),
            ("snk.salesforce", json!({ "object": "Account" }), "object"),
            ("src.rest", json!({ "url": "https://api.example.com/orders" }), "api"),
            ("src.sqlite", json!({ "database": "/data/app.db" }), "database"),
            ("src.clickhouse", json!({ "endpoint": "http://ch:8123" }), "database"),
            ("src.snowflake", json!({ "account": "acme-eu" }), "service"),
            ("src.cassandra", json!({ "contactPoints": "c1:9042,c2:9042" }), "service"),
        ];
        for (component, props, expected_kind) in cases {
            let asset = asset_of(component, &props)
                .unwrap_or_else(|e| panic!("{component} could not be named: {e}"));
            assert_eq!(asset.kind, expected_kind, "{component} landed in the wrong family");
            assert!(!asset.id.is_empty(), "{component} produced an empty name");
            // A doubled separator means a part was formatted in as nothing,
            // which produces a name nothing else will match.
            if let Some((_, rest)) = asset.id.split_once("://") {
                assert!(!rest.contains("//"), "{component} has an empty segment: {}", asset.id);
            }
            assert!(!asset.id.ends_with('/'), "{component} name ends in a slash: {}", asset.id);
        }
    }

    /// The exact strings, for the shapes where getting them subtly wrong would
    /// still look reasonable in a list and silently fail to join.
    ///
    /// A target with no authority is named with one pair of slashes. An earlier
    /// version produced `salesforce:///Account`, which reads fine and is even
    /// valid as a URI, but is a different string from the one every other
    /// authority-less target gets, and a join key that is only nearly right is
    /// a graph with a missing edge.
    #[test]
    fn names_are_exactly_these() {
        let cases: Vec<(&str, Value, &str)> = vec![
            ("snk.salesforce", json!({ "object": "Account" }), "salesforce://Account"),
            ("src.qdrant", json!({ "collection": "embeddings" }), "qdrant://embeddings"),
            (
                "snk.postgres",
                json!({ "host": "db", "port": "5432", "database": "sales", "schema": "public", "tableName": "orders" }),
                "postgres://db:5432/sales.public.orders",
            ),
            (
                "snk.gcs",
                json!({ "bucket": "warehouse", "key": "/curated/a.parquet" }),
                "gcs://warehouse/curated/a.parquet",
            ),
            // A uri that is a local path keeps it, which is the ordinary
            // authority-less URI form and stays stable across pipelines.
            (
                "snk.lancedb",
                json!({ "uri": "/lake/lance", "table": "vectors" }),
                "lancedb:///lake/lance/vectors",
            ),
        ];
        for (component, props, expected) in cases {
            assert_eq!(asset_of(component, &props).unwrap().id, expected, "for {component}");
        }
    }

    #[test]
    fn a_node_with_no_recognisable_target_is_reported_not_dropped() {
        let err = asset_of("src.somethingnew", &json!({ "flavour": "vanilla" })).unwrap_err();
        assert!(err.contains("src.somethingnew"), "the reason must name the component: {err}");
    }

    #[test]
    fn two_pipelines_sharing_a_table_are_connected_without_anyone_linking_them() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let staged = json!({ "path": "/lake/staged.parquet" });

        write_pipeline(
            ws,
            "ingest",
            json!([
                node("n1", "src.postgres", json!({ "host": "db", "database": "sales", "tableName": "orders" })),
                node("n2", "snk.parquet", staged.clone()),
            ]),
        );
        write_pipeline(
            ws,
            "report",
            json!([
                node("n1", "src.parquet", staged),
                node("n2", "snk.csv", json!({ "path": "/out/report.csv" })),
            ]),
        );

        let cat = build(ws).unwrap();
        assert_eq!(cat.pipelines.len(), 2);
        assert_eq!(cat.assets.len(), 3, "expected orders, staged and report");
        assert!(cat.unresolved.is_empty(), "unexpected unresolved: {:?}", cat.unresolved);

        // Nothing in either pipeline file references the other. The connection
        // exists only because they name the same parquet.
        let producers = cat.producers("/lake/staged.parquet");
        let consumers = cat.consumers("/lake/staged.parquet");
        assert_eq!(producers.len(), 1);
        assert_eq!(producers[0].pipeline_id, "ingest");
        assert_eq!(consumers.len(), 1);
        assert_eq!(consumers[0].pipeline_id, "report");
    }

    #[test]
    fn impact_reaches_across_pipelines_and_reports_distance() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        write_pipeline(
            ws,
            "ingest",
            json!([
                node("a", "src.postgres", json!({ "host": "db", "database": "sales", "tableName": "orders" })),
                node("b", "snk.parquet", json!({ "path": "/lake/staged.parquet" })),
            ]),
        );
        write_pipeline(
            ws,
            "enrich",
            json!([
                node("a", "src.parquet", json!({ "path": "/lake/staged.parquet" })),
                node("b", "snk.parquet", json!({ "path": "/lake/enriched.parquet" })),
            ]),
        );
        write_pipeline(
            ws,
            "report",
            json!([
                node("a", "src.parquet", json!({ "path": "/lake/enriched.parquet" })),
                node("b", "snk.csv", json!({ "path": "/out/report.csv" })),
            ]),
        );

        let cat = build(ws).unwrap();
        let hit = cat.impact("postgres://db/sales.orders", None);

        // Dropping a column in the source table reaches all three pipelines,
        // two of which never mention Postgres anywhere.
        let names: Vec<&str> = hit.pipelines.iter().map(|p| p.id.as_str()).collect();
        assert_eq!(names, vec!["enrich", "ingest", "report"]);
        let depth = |id: &str| hit.pipelines.iter().find(|p| p.id == id).unwrap().depth;
        assert_eq!(depth("ingest"), 1, "the pipeline reading it directly is one hop");
        assert_eq!(depth("enrich"), 2);
        assert_eq!(depth("report"), 3);
        assert!(hit.assets.iter().any(|a| a.id == "/out/report.csv"));
    }

    #[test]
    fn a_pipeline_that_reads_and_writes_the_same_asset_does_not_hang_the_walk() {
        // The normal incremental pattern: read a table, write it back. A walk
        // without a visited set never returns from this.
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        write_pipeline(
            ws,
            "accumulate",
            json!([
                node("a", "src.parquet", json!({ "path": "/lake/running.parquet" })),
                node("b", "snk.parquet", json!({ "path": "/lake/running.parquet" })),
            ]),
        );
        let cat = build(ws).unwrap();
        let hit = cat.impact("/lake/running.parquet", None);
        assert_eq!(hit.pipelines.len(), 1);
    }

    #[test]
    fn nodes_that_cannot_be_named_are_counted_on_the_answer() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        write_pipeline(
            ws,
            "partly-known",
            json!([
                node("a", "src.parquet", json!({ "path": "/lake/in.parquet" })),
                node("b", "snk.mysterybox", json!({ "wat": "?" })),
            ]),
        );
        let cat = build(ws).unwrap();
        assert_eq!(cat.unresolved.len(), 1);
        assert_eq!(cat.unresolved[0].node_id, "b");

        // The count rides along on impact, so a caller cannot show the result
        // as exhaustive without also seeing that something was missed.
        assert_eq!(cat.impact("/lake/in.parquet", None).unresolved, 1);
    }

    #[test]
    fn orphans_are_written_but_unread_and_externals_are_read_but_unwritten() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        write_pipeline(
            ws,
            "one",
            json!([
                node("a", "src.csv", json!({ "path": "/in/upstream.csv" })),
                node("b", "snk.csv", json!({ "path": "/out/nobody-reads-this.csv" })),
            ]),
        );
        let cat = build(ws).unwrap();
        let orphans: Vec<&str> = cat.orphans().iter().map(|a| a.id.as_str()).collect();
        assert_eq!(orphans, vec!["/out/nobody-reads-this.csv"]);
        let externals: Vec<&str> = cat.externals().iter().map(|a| a.id.as_str()).collect();
        assert_eq!(externals, vec!["/in/upstream.csv"]);
    }

    #[test]
    fn the_first_matching_rule_wins_so_specific_beats_general() {
        // A file read top to bottom should behave the way it reads. Putting the
        // narrow rule first is how anyone carves an exception out of a broad
        // one, and last-match-wins would silently invert that.
        let owners = Owners {
            assets: vec![
                OwnerRule {
                    maximum_age: None,
                    pattern: "/lake/raw/pii_*".into(),
                    owner: "Privacy".into(),
                    contact: Some("privacy@acme.test".into()),
                    description: None,
                    tags: Vec::new(),
                },
                OwnerRule {
                    maximum_age: None,
                    pattern: "/lake/raw/*".into(),
                    owner: "Data Platform".into(),
                    contact: None,
                    description: None,
                    tags: Vec::new(),
                },
            ],
            pipelines: vec![OwnerRule {
                maximum_age: None,
                pattern: "*-ingest-*".into(),
                owner: "Ingest".into(),
                contact: None,
                description: None,
                tags: Vec::new(),
            }],
            terms: Default::default(),
        };
        assert_eq!(owners.for_asset("/lake/raw/pii_customers.parquet").unwrap().owner, "Privacy");
        assert_eq!(owners.for_asset("/lake/raw/orders.parquet").unwrap().owner, "Data Platform");
        assert!(owners.for_asset("/exports/report.csv").is_none(), "matched something it should not");
        assert_eq!(owners.for_pipeline("01-ingest-orders").unwrap().owner, "Ingest");
    }

    #[test]
    fn a_pattern_that_will_not_compile_owns_nothing() {
        // The dangerous failure is the other way round: a typo that matches
        // everything would hand one team the whole workspace and read as though
        // ownership were complete.
        let owners = Owners {
            assets: vec![OwnerRule {
                maximum_age: None,
                pattern: "[unclosed".into(),
                owner: "Nobody".into(),
                contact: None,
                                   description: None,
            tags: Vec::new(),
                                   }],
            pipelines: vec![],
                            terms: Default::default(),
                            };
        assert!(owners.for_asset("/lake/raw/orders.parquet").is_none());
        assert!(owners.for_asset("anything at all").is_none());
    }

    #[test]
    fn impact_says_who_to_tell_and_unowned_says_what_nobody_claims() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        write_pipeline(
            ws,
            "ingest",
            json!([
                node("a", "src.postgres", json!({ "host": "db", "database": "sales", "tableName": "orders" })),
                node("b", "snk.parquet", json!({ "path": "/lake/raw/orders.parquet" })),
            ]),
        );
        write_pipeline(
            ws,
            "report",
            json!([
                node("a", "src.parquet", json!({ "path": "/lake/raw/orders.parquet" })),
                node("b", "snk.csv", json!({ "path": "/exports/report.csv" })),
            ]),
        );
        std::fs::write(
            owners_path(ws),
            json!({
                "assets": [{ "match": "/lake/raw/*", "owner": "Data Platform", "contact": "dp@acme.test" }],
                "pipelines": [{ "match": "report", "owner": "Analytics" }],
            })
            .to_string(),
        )
        .unwrap();

        let cat = build(ws).unwrap();
        let owners = load_owners(ws).unwrap();
        let hit = cat.impact("postgres://db/sales.orders", Some(&owners));

        let owner_of = |id: &str| {
            hit.pipelines
                .iter()
                .chain(hit.assets.iter())
                .find(|r| r.id == id)
                .unwrap_or_else(|| panic!("{id} was not reached"))
                .owner
                .clone()
        };
        assert_eq!(owner_of("report").as_deref(), Some("Analytics"));
        assert_eq!(owner_of("/lake/raw/orders.parquet").as_deref(), Some("Data Platform"));
        // No rule covers the ingest pipeline or the export, and saying so is
        // the point: a blank owner is a finding, not a formatting problem.
        assert_eq!(owner_of("ingest"), None);
        assert_eq!(owner_of("/exports/report.csv"), None);

        let unowned: Vec<&str> = cat.unowned(&owners).iter().map(|a| a.id.as_str()).collect();
        assert_eq!(unowned, vec!["/exports/report.csv", "postgres://db/sales.orders"]);
    }

    #[test]
    fn a_workspace_with_no_owners_file_reports_everything_unowned() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        write_pipeline(ws, "p", json!([node("a", "src.csv", json!({ "path": "/in/a.csv" }))]));
        let owners = load_owners(ws).unwrap();
        assert!(owners.is_empty());
        assert_eq!(build(ws).unwrap().unowned(&owners).len(), 1);

        // A file that will not parse must not read as "nobody owns anything",
        // which is exactly what an empty result would look like.
        std::fs::write(owners_path(ws), b"{ not json").unwrap();
        assert!(load_owners(ws).is_err());
    }

    #[test]
    fn a_built_catalog_round_trips_through_disk() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        assert!(load(ws).unwrap().is_none(), "nothing built yet");
        write_pipeline(ws, "p", json!([node("a", "src.csv", json!({ "path": "/in/a.csv" }))]));

        let built = build_and_save(ws).unwrap();
        let loaded = load(ws).unwrap().expect("saved catalog");
        assert_eq!(loaded.assets, built.assets);
        assert_eq!(loaded.touches, built.touches);
    }
}
