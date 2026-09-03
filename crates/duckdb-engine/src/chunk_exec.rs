//! #306: chunked extraction, executed on #295's lifecycle.
//!
//! One enormous read becomes N bounded ones that can be retried individually.
//! What is chunk-specific lives here - capability negotiation, the predicates,
//! the snapshot semantics, the staged parts and their assembly - and everything
//! else is deliberately NOT here: requested/running/succeeded/failed, claiming,
//! bounded concurrency, resource-pool admission, reuse of an identical
//! occurrence, retry, restart reconciliation, run ids and receipts all come
//! from [`crate::backfill_exec`], the same code a partitioned backfill uses.
//!
//! That was Louis's framing on the issue and it is the design: a chunk is a
//! slice with a different generator. A second executor beside the first is
//! where the rules quietly diverge, and it is always the second one that
//! forgets to acquire the pool or to record the release.
//!
//! ## The commit rule
//!
//! ```text
//! query completed
//! -> part written, fsynced, hashed, renamed into place
//! -> slice succeeds
//! ```
//!
//! Not "the query finished". A process that dies between the read and the
//! commit would otherwise leave a slice marked done whose part is not there,
//! and the retry that exists to fix exactly that would skip it - silently, and
//! after an hour of database time. [`crate::backfill::commit`] does the
//! ordering; [`SliceWork::requires_artifact`] makes the executor refuse a
//! success without one.

use crate::backfill::{self, Backfill, Kind, PartitionRun, State};
use crate::backfill_exec::{Done, SliceOutcome, SliceWork};
use crate::chunking::{self, Bounds, Strategy};
use serde_json::{json, Value as JsonValue};
use std::path::{Path, PathBuf};

/// The node in the pipeline file that a chunked extract reads.
#[derive(Debug)]
struct Target {
    component: String,
    strategy: Strategy,
    concurrency: usize,
}

fn target_of(doc: &JsonValue, node_id: &str) -> Result<Target, String> {
    let node = doc
        .get("nodes")
        .and_then(|v| v.as_array())
        .and_then(|ns| ns.iter().find(|n| n.get("id").and_then(|v| v.as_str()) == Some(node_id)))
        .ok_or_else(|| format!("no node {node_id:?} in this pipeline"))?;
    let component = node
        .get("data")
        .and_then(|d| d.get("componentId"))
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let props = node
        .get("data")
        .and_then(|d| d.get("properties"))
        .cloned()
        .unwrap_or_else(|| json!({}));
    let spec = props.get("chunking").ok_or_else(|| {
        format!(
            "node {node_id} declares no `chunking`. Without it the source is read with one \
             query, which is the default and is fine until it is not."
        )
    })?;
    let strategy: Strategy =
        serde_json::from_value(spec.clone()).map_err(|e| format!("chunking on {node_id}: {e}"))?;
    // Capability first, before anything else is computed: telling someone how
    // their chunks would look and THEN that the connector cannot do it is the
    // wrong order.
    chunking::check_supported(&component, &strategy)?;
    let concurrency = spec.get("concurrency").and_then(|v| v.as_u64()).unwrap_or(1) as usize;
    Ok(Target { component, strategy, concurrency })
}

/// Where a chunked extract stages its parts.
pub fn staging_dir(workspace: &Path, backfill_id: &str) -> PathBuf {
    workspace.join(".duckle").join("chunks").join(backfill_id)
}

/// Turn a chunk plan into slices, without running any of them.
///
/// The same ledger [`crate::backfill_exec::plan_for`] produces, because it IS
/// the same ledger: `backfill status`, `backfill retry` and the restart
/// reconciliation all work on a chunked extract without knowing it is one.
pub fn plan_for(
    workspace: &Path,
    pipeline_path: &Path,
    node_id: &str,
    bounds: &Bounds,
    nulls: u64,
) -> Result<Backfill, String> {
    let text = std::fs::read_to_string(pipeline_path)
        .map_err(|e| format!("{}: {e}", pipeline_path.display()))?;
    let doc: JsonValue =
        serde_json::from_str(&text).map_err(|e| format!("{}: {e}", pipeline_path.display()))?;
    let t = target_of(&doc, node_id)?;
    let plan = chunking::plan(
        &t.strategy,
        bounds,
        nulls,
        t.concurrency,
        chunking::snapshot_of(&t.component),
        chunking::dialect_of(&t.component),
    )?;

    let name = pipeline_path
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "pipeline".into());
    let release = crate::release::active(
        workspace,
        &std::env::var("DUCKLE_ENVIRONMENT").unwrap_or_else(|_| "default".into()),
    );
    let id = backfill::new_id(&format!("{name}-{node_id}"));
    let staging = staging_dir(workspace, &id);
    Ok(Backfill {
        pipeline: name.clone(),
        pipeline_path: pipeline_path.display().to_string(),
        created_at: chrono::Utc::now().to_rfc3339(),
        release_id: release.clone(),
        max_concurrent: plan.concurrency,
        pid: Some(std::process::id()),
        kind: Kind::Chunk,
        chunk_node: Some(node_id.to_string()),
        staging: Some(staging.display().to_string()),
        partitions: plan
            .chunks
            .iter()
            .map(|c| PartitionRun {
                // What a chunk IS: the node it reads and the range it covers,
                // of this release. Two requests for the same chunk of the same
                // code are the same work, which is what lets a restart find
                // that it is already done (#295).
                occurrence: Some(backfill::occurrence_id(
                    &name,
                    &format!("chunk:{node_id}:{}", c.key),
                    release.as_deref(),
                    None,
                )),
                key: c.key.clone(),
                state: State::Requested,
                run_id: None,
                attempts: 0,
                error: None,
                finished_at: None,
                params: Default::default(),
                predicate: Some(c.predicate.clone()),
                artifact: None,
            })
            .collect(),
        id,
    })
}

/// Ask the source how far the key runs, and whether it is nullable.
///
/// #306 lists the probe as part of the chunk layer, and it was the one part
/// still being done by hand: `source plan` printed the SQL and the operator ran
/// it and typed the numbers back. That is fine once and wrong every time after,
/// because the numbers go stale the moment the table grows and nothing notices.
///
/// It runs the same way a chunk does - one synthetic pipeline through the
/// ordinary engine - so it inherits the node's prelude, its secrets, its
/// resource pool and its connection exactly as the extract will. A probe that
/// reached the source by some other path could succeed where the extract then
/// fails, which is the least useful way to be right.
pub fn probe(
    workspace: &Path,
    duckdb: &Path,
    pipeline_path: &Path,
    node_id: &str,
) -> Result<(Bounds, u64), String> {
    let text = std::fs::read_to_string(pipeline_path)
        .map_err(|e| format!("{}: {e}", pipeline_path.display()))?;
    let doc: JsonValue =
        serde_json::from_str(&text).map_err(|e| format!("{}: {e}", pipeline_path.display()))?;
    let t = target_of(&doc, node_id)?;
    let node = node_of(&doc, node_id)?;
    let props = node
        .get("data")
        .and_then(|d| d.get("properties"))
        .cloned()
        .unwrap_or_else(|| json!({}));

    // The node's own read, as the engine builds it, wrapped so MIN/MAX apply to
    // whatever it produces - a table, or the author's SQL.
    let mut probe_props = constrain(&t.component, &props, "TRUE")?;
    let base = probe_props
        .get("sql")
        .and_then(|v| v.as_str())
        .ok_or("the probe could not build a read for this node")?
        .to_string();
    let sql = chunking::probe_sql_over(&t.strategy, &format!("({base}) AS duckle_probe"))?;
    if let Some(o) = probe_props.as_object_mut() {
        o.insert("sql".into(), json!(sql));
    }

    let out = std::env::temp_dir().join(format!(
        "duckle_probe_{}_{}.parquet",
        std::process::id(),
        node_id.chars().filter(|c| c.is_ascii_alphanumeric()).collect::<String>()
    ));
    let _ = std::fs::remove_file(&out);
    let mut probe_doc = doc.clone();
    let mut probe_node = node.clone();
    probe_node["data"]["properties"] = probe_props;
    probe_doc["nodes"] = json!([
        probe_node,
        {
            "id": "duckle_probe_out",
            "type": "sink",
            "position": { "x": 320, "y": 0 },
            "data": {
                "label": "probe",
                "componentId": "snk.parquet",
                "properties": { "path": forward_slashes(&out) }
            }
        }
    ]);
    probe_doc["edges"] =
        json!([{ "id": "duckle_probe_edge", "source": node_id, "target": "duckle_probe_out" }]);
    let probe_doc: crate::PipelineDoc = serde_json::from_value(probe_doc)
        .map_err(|e| format!("building the probe's document: {e}"))?;

    let engine = crate::DuckdbEngine::new(duckdb.to_path_buf()).without_previews();
    let result = engine.execute_pipeline_named(&probe_doc, "probe");
    if result.status != "ok" {
        let _ = std::fs::remove_file(&out);
        return Err(format!(
            "probing {node_id} failed: {}",
            result.error.unwrap_or_else(|| "the probe run failed".into())
        ));
    }
    let rows = engine
        .run_rows(None, &format!("SELECT * FROM read_parquet('{}')", forward_slashes(&out)))
        .map_err(|e| format!("reading the probe result: {e}"))?;
    let _ = std::fs::remove_file(&out);
    let row = rows.first().ok_or("the probe returned no rows")?;

    let nulls = row.get("nulls").and_then(num).unwrap_or(0.0).max(0.0) as u64;
    let bounds = match &t.strategy {
        Strategy::Hash { .. } => Bounds::None,
        Strategy::Range { .. } => {
            let (Some(lo), Some(hi)) = (row.get("lo").and_then(num), row.get("hi").and_then(num))
            else {
                // Distinguished from a bad answer: an empty table is a fact
                // about the table, not a failure of the probe.
                return Err(format!(
                    "{} has no non-null values in {}, so there is nothing to chunk",
                    node_id,
                    t.strategy.column()
                ));
            };
            Bounds::Range { min: lo as i64, max: hi as i64 }
        }
        Strategy::Time { .. } => {
            let day = |v: Option<&JsonValue>| {
                v.and_then(|v| v.as_str())
                    .map(|s| s.chars().take(10).collect::<String>())
                    .filter(|s| s.len() == 10)
            };
            let (Some(from), Some(to)) = (day(row.get("lo")), day(row.get("hi"))) else {
                return Err(format!(
                    "{} has no non-null values in {}, so there is nothing to chunk",
                    node_id,
                    t.strategy.column()
                ));
            };
            Bounds::Time { from, to }
        }
    };
    Ok((bounds, nulls))
}

/// A number out of a probe row, whichever way DuckDB spelled it.
fn num(v: &JsonValue) -> Option<f64> {
    v.as_f64().or_else(|| v.as_str().and_then(|s| s.trim().parse().ok()))
}

fn node_of<'a>(doc: &'a JsonValue, node_id: &str) -> Result<&'a JsonValue, String> {
    doc.get("nodes")
        .and_then(|v| v.as_array())
        .and_then(|ns| ns.iter().find(|n| n.get("id").and_then(|v| v.as_str()) == Some(node_id)))
        .ok_or_else(|| format!("no node {node_id:?} in this pipeline"))
}

/// Restrict a source node to one chunk.
///
/// Rewrites the node's READ rather than filtering after it: a filter applied on
/// this side would make every chunk fetch the whole table, which is the thing
/// chunking exists to stop.
pub(crate) fn constrain(
    component_id: &str,
    props: &JsonValue,
    predicate: &str,
) -> Result<JsonValue, String> {
    let mut out = props.clone();
    let obj = out
        .as_object_mut()
        .ok_or("this node has no properties to constrain")?;
    obj.remove("chunking");

    let authored = ["sql", "query"]
        .iter()
        .find_map(|k| props.get(*k).and_then(|v| v.as_str()))
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string);

    match (authored, crate::plan::relational_pushdown_on(props)) {
        // Pushdown on, with the author's own SQL: the whole statement runs on
        // the remote server, so the predicate goes INSIDE it and stays there.
        // Wrapping the outside would fetch the unchunked result and filter it
        // here, once per chunk, which is worse than not chunking at all.
        (Some(sql), true) => {
            let inner = sql.trim_end_matches(';').trim().to_string();
            obj.insert(
                "sql".into(),
                json!(format!("SELECT * FROM ({inner}) duckle_chunk WHERE {predicate}")),
            );
            obj.remove("query");
        }
        // Otherwise the read is a DuckDB-side relation over the attached
        // source, and the predicate goes around it. Pushdown is turned off
        // explicitly: the rewritten SQL names the attach alias, which the
        // remote server has never heard of, so sending it there would fail.
        _ => {
            // The engine's OWN dispatch, not a second one: `src.duckdb` and
            // `src.postgres` do not build their reads the same way, and a
            // chunk that guessed would be reading something other than the
            // node it claims to be chunking.
            let base = crate::plan::build_view_sql(
                component_id,
                props,
                &crate::plan::NodeInputs::default(),
                None,
                false,
            )?;
            let base = base.trim().to_string();
            let from = match base.starts_with('(') && base.ends_with(')') {
                true => base,
                false => format!("({base})"),
            };
            obj.insert(
                "sql".into(),
                json!(format!("SELECT * FROM {from} AS duckle_chunk WHERE {predicate}")),
            );
            obj.remove("query");
            obj.insert("pushdown".into(), json!(false));
        }
    }
    obj.insert("mode".into(), json!("sql"));
    // And the table name has to GO, not merely be overridden. `src.duckdb`
    // reads `tableName` before `sql`, so leaving it behind makes the rewrite
    // dead code: every chunk reads the whole table, every chunk returns
    // everything, and nothing anywhere reports it. The families disagree about
    // which key wins, so the only safe rule is to leave exactly one of them.
    obj.remove("tableName");
    obj.remove("table");
    Ok(out)
}

/// The document one chunk runs: the constrained source, writing one part.
fn extract_doc(
    original: &JsonValue,
    node_id: &str,
    predicate: &str,
    part: &Path,
) -> Result<crate::PipelineDoc, String> {
    let mut doc = original.clone();
    let mut node = doc
        .get("nodes")
        .and_then(|v| v.as_array())
        .and_then(|ns| ns.iter().find(|n| n.get("id").and_then(|v| v.as_str()) == Some(node_id)))
        .cloned()
        .ok_or_else(|| format!("no node {node_id:?} in this pipeline"))?;
    let component = node
        .get("data")
        .and_then(|d| d.get("componentId"))
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let props = node
        .get("data")
        .and_then(|d| d.get("properties"))
        .cloned()
        .unwrap_or_else(|| json!({}));
    node["data"]["properties"] = constrain(&component, &props, predicate)?;

    // Everything else on the document is kept: the resource pool it is queued
    // in, its parameters, its context. A chunk is this pipeline's read, not a
    // different pipeline that happens to resemble it.
    doc["nodes"] = json!([
        node,
        {
            "id": "duckle_chunk_part",
            "type": "sink",
            "position": { "x": 320, "y": 0 },
            "data": {
                "label": "chunk part",
                "componentId": "snk.parquet",
                "properties": { "path": forward_slashes(part) }
            }
        }
    ]);
    doc["edges"] = json!([
        { "id": "duckle_chunk_edge", "source": node_id, "target": "duckle_chunk_part" }
    ]);
    serde_json::from_value(doc).map_err(|e| format!("building the chunk's document: {e}"))
}

fn forward_slashes(p: &Path) -> String {
    p.display().to_string().replace(char::from(92), "/")
}

struct ChunkWork {
    workspace: PathBuf,
    duckdb: PathBuf,
    path: PathBuf,
    original: JsonValue,
    pipeline: String,
    backfill_id: String,
    node_id: String,
    staging: PathBuf,
    release: Option<String>,
    gates: crate::pools::Gates,
}

impl SliceWork for ChunkWork {
    /// #306: a chunk that committed nothing has not succeeded, whatever the
    /// query returned.
    fn requires_artifact(&self) -> bool {
        true
    }

    fn run(&self, slice: &PartitionRun) -> Result<Done, (Option<String>, String)> {
        let predicate = slice.predicate.as_deref().ok_or_else(|| {
            (None, "this slice has no predicate, so there is no bounded read to run".to_string())
        })?;
        std::fs::create_dir_all(&self.staging).map_err(|e| (None, e.to_string()))?;
        let stem = part_name(slice);
        // Written under a temp name and renamed only once it is whole, so a
        // kill mid-write never leaves a part at the path that means "done".
        let tmp = self.staging.join(format!("{stem}.writing"));
        let final_path = self.staging.join(format!("{stem}.parquet"));
        let _ = std::fs::remove_file(&tmp);

        let doc =
            extract_doc(&self.original, &self.node_id, predicate, &tmp).map_err(|e| (None, e))?;
        let (run_id, rows) = crate::backfill_exec::run_doc(
            &self.workspace,
            &self.duckdb,
            doc,
            &self.path,
            &self.pipeline,
            &self.backfill_id,
            slice,
            &self.release,
            &self.gates,
            "chunk",
        )?;
        let artifact = backfill::commit(&tmp, &final_path, rows)
            .map_err(|e| (Some(run_id.clone()), format!("the read finished but {e}")))?;
        Ok(Done { run_id, artifact: Some(artifact) })
    }
}

/// A filesystem-safe name for a chunk's part.
///
/// `bucket 7 of 64` and `1..1000000` are for people to read, and neither is a
/// filename.
fn part_name(slice: &PartitionRun) -> String {
    let safe: String = slice
        .key
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    format!("part-{safe}")
}

/// Run a chunked extract.
pub fn execute(
    workspace: &Path,
    duckdb: &Path,
    plan: Backfill,
    force: bool,
    on_slice: &(dyn Fn(SliceOutcome) + Sync),
) -> Result<Backfill, String> {
    let path = PathBuf::from(&plan.pipeline_path);
    let text = std::fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))?;
    let original: JsonValue =
        serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?;
    let node_id = plan
        .chunk_node
        .clone()
        .ok_or("this ledger is not a chunked extract: it names no source node")?;
    let staging = plan
        .staging
        .clone()
        .map(PathBuf::from)
        .unwrap_or_else(|| staging_dir(workspace, &plan.id));

    // #306: a chunk is only still done while its part is. Checked on the way
    // in rather than left to whoever calls this, so the CLI, the console and
    // MCP all get it - a resumability guarantee that only holds when the
    // operator remembers to ask for it is not one. The cheap check: existence
    // and length, which is exactly what a crash between the read and the
    // commit produces. `recheck_artifacts(true)` re-reads every part and is a
    // deliberate act, because on a chunked extract it costs a whole extract.
    let mut plan = plan;
    if !plan.recheck_artifacts(false).is_empty() {
        let _ = backfill::save(workspace, &plan);
    }
    let work = ChunkWork {
        workspace: workspace.to_path_buf(),
        duckdb: duckdb.to_path_buf(),
        path,
        original,
        pipeline: plan.pipeline.clone(),
        backfill_id: plan.id.clone(),
        node_id,
        staging,
        release: plan.release_id.clone(),
        gates: crate::pools::Gates::load(workspace),
    };
    Ok(crate::backfill_exec::execute_with(workspace, plan, force, &work, on_slice))
}

/// The relation every committed part makes together.
///
/// The assembly step of #306: once every chunk has succeeded, the extract IS
/// the parts read as one. Nothing is copied or merged to get there, so
/// assembling costs nothing and the parts stay individually replaceable.
pub fn assembled_read(plan: &Backfill) -> Result<String, String> {
    if !plan.is_done() {
        return Err(format!(
            "{} of {} chunks are not done, and a partial extract read as though it were whole is \
             the failure this exists to prevent",
            plan.partitions.iter().filter(|p| p.state.is_open()).count(),
            plan.partitions.len()
        ));
    }
    let mut parts: Vec<String> = Vec::new();
    for p in &plan.partitions {
        match &p.artifact {
            Some(a) => parts.push(format!("'{}'", a.uri.replace('\'', "''"))),
            None => {
                return Err(format!(
                    "chunk {} is marked succeeded with no committed part, so the extract is \
                     incomplete and the ledger cannot say by how much",
                    p.key
                ))
            }
        }
    }
    Ok(format!("SELECT * FROM read_parquet([{}])", parts.join(", ")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backfill::SliceArtifact;

    fn slice(key: &str, state: State, part: Option<&str>) -> PartitionRun {
        PartitionRun {
            key: key.into(),
            state,
            run_id: None,
            attempts: 1,
            error: None,
            finished_at: None,
            params: Default::default(),
            occurrence: None,
            predicate: Some("id >= 0".into()),
            artifact: part.map(|u| SliceArtifact {
                uri: u.into(),
                hash: "abc".into(),
                bytes: 1,
                rows: None,
            }),
        }
    }

    fn chunked(parts: Vec<PartitionRun>) -> Backfill {
        Backfill {
            id: "bf-x".into(),
            pipeline: "extract".into(),
            pipeline_path: "p.json".into(),
            created_at: "2026-09-02T00:00:00Z".into(),
            release_id: None,
            max_concurrent: 2,
            pid: None,
            kind: Kind::Chunk,
            chunk_node: Some("pg".into()),
            staging: None,
            partitions: parts,
        }
    }

    /// The predicate has to end up in the READ. Filtering afterwards would make
    /// every chunk fetch the whole table, which is what chunking exists to stop.
    #[test]
    fn a_table_read_is_narrowed_at_the_source() {
        let props = json!({ "host": "db", "database": "sales", "tableName": "orders" });
        let out = constrain("src.postgres", &props, "id >= 0 AND id < 100").unwrap();
        let sql = out.get("sql").and_then(|v| v.as_str()).unwrap_or_default();
        assert!(sql.contains("orders"), "the table is gone from the read: {sql}");
        assert!(sql.contains("id >= 0 AND id < 100"), "the predicate is not in the read: {sql}");
        assert_eq!(out.get("mode").and_then(|v| v.as_str()), Some("sql"));
    }

    /// The rewritten SQL names the ATTACH alias, which the remote server has
    /// never heard of. Leaving pushdown on would send it there and fail.
    #[test]
    fn a_rewritten_table_read_is_not_sent_to_the_remote_server() {
        let props = json!({
            "host": "db", "database": "sales", "tableName": "orders", "pushdown": true
        });
        let out = constrain("src.postgres", &props, "id < 100").unwrap();
        let sql = out.get("sql").and_then(|v| v.as_str()).unwrap_or_default();
        assert!(sql.contains("duckle_src"), "expected the attach alias: {sql}");
        assert_eq!(
            out.get("pushdown"),
            Some(&json!(false)),
            "the attach alias would be sent to a server that cannot resolve it: {sql}"
        );
    }

    /// And the opposite case: the author's own SQL with pushdown on runs
    /// entirely on the remote server, so the predicate must go INSIDE it.
    #[test]
    fn a_pushed_down_query_keeps_the_predicate_on_the_server() {
        let props = json!({
            "host": "db", "database": "sales", "pushdown": true,
            "query": "SELECT id, total FROM orders"
        });
        let out = constrain("src.postgres", &props, "id < 100").unwrap();
        let sql = out.get("sql").and_then(|v| v.as_str()).unwrap_or_default();
        assert!(sql.contains("SELECT id, total FROM orders"), "{sql}");
        assert!(sql.contains("WHERE id < 100"), "{sql}");
        assert!(
            !sql.contains("duckle_src"),
            "a remote query was rewritten to name a DuckDB-side alias: {sql}"
        );
        assert_ne!(
            out.get("pushdown"),
            Some(&json!(false)),
            "pushdown was turned off, so every chunk would fetch the whole result and filter here"
        );
    }

    /// The one that matters, asserted on what the ENGINE will actually run
    /// rather than on the property that was set.
    ///
    /// `src.duckdb` reads `tableName` BEFORE `sql`, so leaving the table name
    /// behind makes the rewritten SQL dead: the chunk reads the whole table,
    /// every chunk returns everything, and nothing anywhere says so. Checking
    /// the `sql` property would have passed against exactly that.
    #[test]
    fn the_predicate_reaches_the_sql_the_engine_runs() {
        let cases = [
            ("src.duckdb", json!({ "path": "w.duckdb", "tableName": "orders" })),
            ("src.postgres", json!({ "host": "d", "database": "s", "tableName": "orders" })),
            ("src.ducklake", json!({ "database": "s", "tableName": "orders" })),
        ];
        for (component, props) in cases {
            let constrained = constrain(component, &props, "id >= 0 AND id < 100").unwrap();
            let sql = crate::plan::build_view_sql(
                component,
                &constrained,
                &crate::plan::NodeInputs::default(),
                None,
                false,
            )
            .unwrap_or_else(|e| panic!("{component}: {e}"));
            assert!(
                sql.contains("id >= 0 AND id < 100"),
                "{component} would read the whole table: {sql}"
            );
        }
    }

    /// The chunking spec itself must not survive into the chunk's own read, or
    /// the extract would try to chunk its own chunk.
    #[test]
    fn the_chunking_spec_does_not_survive_into_the_chunk() {
        let props = json!({
            "host": "db", "database": "s", "tableName": "t",
            "chunking": { "type": "range", "column": "id", "chunkSize": 10 }
        });
        let out = constrain("src.postgres", &props, "id < 10").unwrap();
        assert!(out.get("chunking").is_none(), "the chunk still declares chunking");
    }

    /// A part name is a filename, and neither `bucket 7 of 64` nor `1..1000000`
    /// is one.
    #[test]
    fn a_part_name_is_usable_as_a_filename() {
        for key in ["1..1000000", "bucket 7 of 64", "2020-03"] {
            let n = part_name(&slice(key, State::Requested, None));
            assert!(
                !n.contains(['.', ' ', '/', ':']),
                "{n} is not a filename"
            );
        }
    }

    /// Reading a partial extract as though it were whole is the exact failure
    /// resumability exists to prevent.
    #[test]
    fn a_partial_extract_cannot_be_assembled() {
        let plan = chunked(vec![
            slice("a", State::Succeeded, Some("a.parquet")),
            slice("b", State::Failed, None),
        ]);
        let e = assembled_read(&plan).expect_err("a partial extract was assembled");
        assert!(e.contains("1 of 2"), "{e}");
    }

    /// And the subtler one: every chunk says succeeded, but one has no part.
    /// Without this the assembled read is short and nothing says by how much.
    #[test]
    fn a_succeeded_chunk_with_no_part_cannot_be_assembled() {
        let plan = chunked(vec![
            slice("a", State::Succeeded, Some("a.parquet")),
            slice("b", State::Succeeded, None),
        ]);
        let e = assembled_read(&plan).expect_err("an extract missing a part was assembled");
        assert!(e.contains("chunk b"), "{e}");
    }

    #[test]
    fn a_complete_extract_reads_every_part() {
        let plan = chunked(vec![
            slice("a", State::Succeeded, Some("a.parquet")),
            slice("b", State::Succeeded, Some("b.parquet")),
        ]);
        let sql = assembled_read(&plan).unwrap();
        assert!(sql.contains("'a.parquet'") && sql.contains("'b.parquet'"), "{sql}");
    }

    /// A resumability guarantee that only holds when the operator remembers to
    /// ask for it is not one, so running an extract rechecks its own parts
    /// before it decides there is nothing to do.
    ///
    /// Nothing here resets the slice by hand: the ledger says succeeded and the
    /// part is not there, which is exactly the state a crash between the read
    /// and the commit leaves behind. The run then fails (there is no DuckDB
    /// here), and failing is the point - it was CLAIMED, where before it would
    /// have been skipped as done.
    #[test]
    fn running_an_extract_notices_a_part_that_is_gone() {
        let tmp = tempfile::tempdir().unwrap();
        let pipeline = tmp.path().join("extract.json");
        std::fs::write(
            &pipeline,
            serde_json::to_string(&json!({
                "nodes": [{
                    "id": "pg",
                    "position": { "x": 0, "y": 0 },
                    "data": {
                        "label": "pg",
                        "componentId": "src.duckdb",
                        "properties": { "database": "w.duckdb", "tableName": "orders" }
                    }
                }],
                "edges": []
            }))
            .unwrap(),
        )
        .unwrap();

        let mut plan = chunked(vec![slice(
            "a",
            State::Succeeded,
            Some(&tmp.path().join("gone.parquet").display().to_string()),
        )]);
        plan.pipeline_path = pipeline.display().to_string();
        assert!(plan.is_done(), "the ledger should start out claiming to be complete");

        let done = execute(tmp.path(), Path::new("no-such-duckdb-binary"), plan, false, &|_| {})
            .expect("running the extract");
        assert_ne!(
            done.partitions[0].state,
            State::Succeeded,
            "a chunk whose part is gone was left as succeeded, so a retry would skip it"
        );
    }

    /// #306 asks that a connector which cannot give stable semantics refuse
    /// rather than emulate them.
    #[test]
    fn a_connector_that_cannot_chunk_refuses() {
        let doc = json!({ "nodes": [{
            "id": "csv",
            "data": {
                "componentId": "src.csv",
                "properties": { "chunking": { "type": "range", "column": "id", "chunkSize": 10 } }
            }
        }]});
        let e = target_of(&doc, "csv").expect_err("src.csv accepted a chunking strategy");
        assert!(e.contains("does not support chunked extraction"), "{e}");
    }
}
