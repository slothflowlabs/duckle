//! #295: the one place a backfill's slices are executed.
//!
//! In the engine rather than in the CLI so the command, the console and MCP
//! share it. Louis's concern on #295 was exactly this: a backfill executor
//! beside the normal durable-run path becomes a second orchestration, and the
//! second one is where the rules quietly differ - a slice that skips its
//! resource pool, a receipt that forgets its release.

use crate::backfill::{self, Backfill, PartitionRun, State};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// What happened to one slice, as it happens.
pub struct SliceOutcome {
    pub key: String,
    pub run_id: Option<String>,
    pub error: Option<String>,
    /// The backfill whose run this slice reused instead of doing the work
    /// again (#295).
    pub reused_from: Option<String>,
    /// What the slice committed, when it produces output (#306).
    pub artifact: Option<backfill::SliceArtifact>,
}

/// What one slice produced.
pub struct Done {
    pub run_id: String,
    /// The durable output, already committed and hashed. A slice whose work
    /// declares [`SliceWork::requires_artifact`] is NOT marked succeeded
    /// without one (#306).
    pub artifact: Option<backfill::SliceArtifact>,
}

/// What one slice DOES.
///
/// #306: a chunk is a slice with a different generator, so everything around it
/// is the same - claiming, saving before starting, bounded concurrency,
/// resource-pool admission, reuse of an identical occurrence, retry, restart
/// reconciliation. Only this differs. Writing a second executor to say so is
/// how the two would quietly come to disagree, which is the thing Louis raised
/// on #295 and again on #306.
pub trait SliceWork: Sync {
    /// Run one slice. `Err` carries the run id when there was one, so a failure
    /// is still traceable to its receipt and log.
    fn run(&self, slice: &PartitionRun) -> Result<Done, (Option<String>, String)>;

    /// Whether a slice of this kind must have committed a durable output before
    /// it may be called succeeded.
    ///
    /// #306, and the part that is easy to get wrong: "the query completed" is
    /// not "the slice succeeded". Enforced HERE rather than trusted to each
    /// implementation, because the cost of getting it wrong is a slice marked
    /// done whose output is missing and a retry that skips it.
    fn requires_artifact(&self) -> bool {
        false
    }
}

/// Build a plan from a pipeline and a range, without running it (#295).
///
/// Separate from `execute` so a dry run is the same code path minus the
/// running: an agent asking "what would this queue" gets the exact keys the
/// executor would take, not a second generator's opinion of them.
pub fn plan_for(
    workspace: &Path,
    pipeline_path: &Path,
    from: &str,
    to: &str,
    max_concurrent: usize,
    // #295: the schedule occurrence that caused this, when one did. Supplied by
    // the caller rather than invented here - #296/#318 own what an occurrence
    // is and which zone it is in; this only consumes the identity.
    schedule_occurrence: Option<&str>,
) -> Result<Backfill, String> {
    let text = std::fs::read_to_string(pipeline_path)
        .map_err(|e| format!("{}: {e}", pipeline_path.display()))?;
    let raw: serde_json::Value =
        serde_json::from_str(&text).map_err(|e| format!("{}: {e}", pipeline_path.display()))?;
    let def = crate::partition::of(&raw).ok_or_else(|| {
        format!(
            "{} declares no `partition`, so there is nothing to slice it by",
            pipeline_path.display()
        )
    })?;
    let parts = crate::partition::generate(&def, from, to)?;
    if parts.is_empty() {
        return Err("that range produces no partitions".to_string());
    }
    let name = pipeline_path
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "pipeline".into());
    let release = crate::release::active(
        workspace,
        &std::env::var("DUCKLE_ENVIRONMENT").unwrap_or_else(|_| "default".into()),
    );
    Ok(Backfill {
        id: backfill::new_id(&name),
        pipeline: name.clone(),
        pipeline_path: pipeline_path.display().to_string(),
        created_at: chrono::Utc::now().to_rfc3339(),
        release_id: release.clone(),
        max_concurrent: max_concurrent.max(1),
        pid: Some(std::process::id()),
        kind: backfill::Kind::Partition,
        chunk_node: None,
        staging: None,
        epoch: None,
        partitions: parts
            .into_iter()
            .map(|p| PartitionRun {
                occurrence: Some(backfill::occurrence_id(
                    &name,
                    &p.key,
                    release.as_deref(),
                    schedule_occurrence,
                )),
                key: p.key,
                state: State::Requested,
                run_id: None,
                attempts: 0,
                error: None,
                finished_at: None,
                params: p.params,
                predicate: None,
                artifact: None,
                requires: None,
                source_uri: None,
            })
            .collect(),
    })
}

/// Run every open partition, `max_concurrent` at a time.
///
/// Each slice is an ordinary run: its own receipt, its own release id, its own
/// log lines. The plan is saved after every state change rather than at the end,
/// because the whole point is that a kill halfway through leaves a resumable
/// record rather than an unanswerable question.
pub fn execute(
    workspace: &Path,
    duckdb: &Path,
    plan: Backfill,
    // Run every slice even if an identical one already succeeded. The escape
    // hatch #295 asks for: sometimes a slice must genuinely be redone.
    force: bool,
    on_slice: &(dyn Fn(SliceOutcome) + Sync),
) -> Backfill {
    let work = PartitionWork {
        workspace: workspace.to_path_buf(),
        duckdb: duckdb.to_path_buf(),
        path: PathBuf::from(&plan.pipeline_path),
        pipeline: plan.pipeline.clone(),
        backfill_id: plan.id.clone(),
        release: plan.release_id.clone(),
        // #295: a backfill's own bound is an ADDITIONAL ceiling, not a way
        // around the machine's. Each slice still acquires the pool its pipeline
        // asks for, so `--max-concurrent 4` over a pipeline in a pool of 1 runs
        // one at a time.
        gates: crate::pools::Gates::load(workspace),
    };
    execute_with(workspace, plan, force, &work, on_slice)
}

/// Run a ledger, whichever kind of slice it holds.
///
/// #306: `backfill status`, `backfill retry` and the restart reconciliation all
/// work on a chunked extract because it is the same ledger - but running one is
/// the one place the two DO differ, and a chunk ledger sent down the partition
/// path would run the whole pipeline once per chunk with no predicate at all.
/// So the dispatch lives here, once, rather than at each of the five callers
/// that would each have to remember it.
pub fn execute_ledger(
    workspace: &Path,
    duckdb: &Path,
    plan: Backfill,
    force: bool,
    on_slice: &(dyn Fn(SliceOutcome) + Sync),
) -> Result<Backfill, String> {
    match plan.kind {
        backfill::Kind::Partition => Ok(execute(workspace, duckdb, plan, force, on_slice)),
        backfill::Kind::Chunk => {
            crate::chunk_exec::execute(workspace, duckdb, plan, force, on_slice)
        }
        // #326: a link of an ordered chain is a run of the pipeline with the
        // object bound, which is the partition path - a slice binding params.
        // What makes it ordered is the claim predicate, not the executor, which
        // is the whole point of not adding a second one.
        backfill::Kind::Sequence => Ok(execute(workspace, duckdb, plan, force, on_slice)),
    }
}

/// One partitioned slice: an ordinary durable run with its parameters bound.
struct PartitionWork {
    workspace: PathBuf,
    duckdb: PathBuf,
    path: PathBuf,
    pipeline: String,
    backfill_id: String,
    release: Option<String>,
    gates: crate::pools::Gates,
}

impl SliceWork for PartitionWork {
    fn run(&self, slice: &PartitionRun) -> Result<Done, (Option<String>, String)> {
        run_one(
            &self.workspace,
            &self.duckdb,
            &self.path,
            &self.pipeline,
            &self.backfill_id,
            slice,
            &self.release,
            &self.gates,
        )
        .map(|run_id| Done { run_id, artifact: None })
    }
}

/// The lifecycle, for any kind of slice.
///
/// Everything a slice needs that is not the slice itself: claiming one under a
/// lock and saying so on disk before starting, bounded concurrency, reuse of an
/// identical occurrence, saving after every state change, and the rule that a
/// slice which must produce output is not succeeded until it has.
pub fn execute_with(
    workspace: &Path,
    plan: Backfill,
    force: bool,
    work: &dyn SliceWork,
    on_slice: &(dyn Fn(SliceOutcome) + Sync),
) -> Backfill {
    let workers = plan
        .max_concurrent
        .min(plan.claimable_count())
        .max(1);
    let requires_artifact = work.requires_artifact();
    let shared = Arc::new(Mutex::new(plan));

    std::thread::scope(|scope| {
        for _ in 0..workers {
            let shared = Arc::clone(&shared);
            let workspace = workspace.to_path_buf();
            scope.spawn(move || loop {
                // Claim one slice under the lock and mark it running on disk
                // before starting, so a crash leaves it `running` for the next
                // start to reconcile rather than looking untouched.
                let claimed = {
                    let mut plan = shared.lock().unwrap_or_else(|p| p.into_inner());
                    // #326: an ordered chain adds a predecessor requirement
                    // to the same claim, so a link whose predecessor has not
                    // landed is passed over rather than run out of order.
                    let Some(idx) = (0..plan.partitions.len()).find(|i| plan.claimable(*i)) else {
                        return;
                    };
                    plan.partitions[idx].state = State::Running;
                    plan.partitions[idx].attempts += 1;
                    let _ = backfill::save(&workspace, &plan);
                    (idx, plan.partitions[idx].clone())
                };
                let (idx, slice) = claimed;
                // #295: this exact slice of this exact release may already have
                // been done - by a restart recreating the plan, or by two
                // schedules firing the same occurrence. Doing it again is not
                // just wasted hours; for a sink without idempotent writes it is
                // duplicated data.
                let done_already = match force {
                    true => None,
                    false => slice
                        .occurrence
                        .as_deref()
                        .and_then(|o| backfill::already_succeeded(&workspace, o)),
                };
                // #306: and reusing it means reusing its OUTPUT. A slice whose
                // work must produce one is only reusable while that output is
                // still there and still the size it was committed at - which is
                // the same rule a restart applies, applied at the other end.
                let done_already = done_already.filter(|(_, prior)| match requires_artifact {
                    false => true,
                    true => prior.artifact.as_ref().is_some_and(|a| {
                        std::fs::metadata(&a.uri).is_ok_and(|m| m.len() == a.bytes)
                    }),
                });
                if let Some((other, prior)) = done_already {
                    let mut plan = shared.lock().unwrap_or_else(|p| p.into_inner());
                    let p = &mut plan.partitions[idx];
                    p.state = State::Succeeded;
                    p.run_id = prior.run_id.clone();
                    p.artifact = prior.artifact.clone();
                    p.finished_at = Some(chrono::Utc::now().to_rfc3339());
                    p.error = None;
                    let told = SliceOutcome {
                        key: slice.key.clone(),
                        run_id: prior.run_id,
                        error: None,
                        reused_from: Some(other),
                        artifact: prior.artifact,
                    };
                    let _ = backfill::save(&workspace, &plan);
                    drop(plan);
                    on_slice(told);
                    continue;
                }
                let outcome = work.run(&slice);
                {
                    let mut plan = shared.lock().unwrap_or_else(|p| p.into_inner());
                    let p = &mut plan.partitions[idx];
                    p.finished_at = Some(chrono::Utc::now().to_rfc3339());
                    match outcome {
                        // #306, the rule that is easy to get wrong: the query
                        // finishing is not the slice succeeding. A slice that
                        // must commit an output and did not is a FAILURE, not a
                        // success with nothing to show - because the difference
                        // between them is a retry that redoes the work and a
                        // retry that skips it.
                        Ok(Done { run_id, artifact }) if requires_artifact && artifact.is_none() => {
                            p.state = State::Failed;
                            p.run_id = Some(run_id);
                            p.error = Some(
                                "the read finished but no output was committed, so there is                                  nothing to reuse and the slice is not done"
                                    .to_string(),
                            );
                        }
                        Ok(Done { run_id, artifact }) => {
                            p.state = State::Succeeded;
                            p.run_id = Some(run_id);
                            p.artifact = artifact;
                            p.error = None;
                        }
                        Err((run_id, e)) => {
                            p.state = State::Failed;
                            p.run_id = run_id;
                            p.error = Some(e);
                        }
                    }
                    // Copied out before the save, so the callback does not
                    // hold a borrow of the plan while it is written.
                    let told = SliceOutcome {
                        key: slice.key.clone(),
                        run_id: p.run_id.clone(),
                        error: p.error.clone(),
                        reused_from: None,
                        artifact: p.artifact.clone(),
                    };
                    let _ = backfill::save(&workspace, &plan);
                    drop(plan);
                    // Outside the lock: a caller printing a line, or writing to
                    // a socket, must not hold up the other workers.
                    on_slice(told);
                }
            });
        }
    });

    let mut plan = Arc::try_unwrap(shared)
        .map(|m| m.into_inner().unwrap_or_else(|p| p.into_inner()))
        .unwrap_or_else(|arc| arc.lock().unwrap_or_else(|p| p.into_inner()).clone());
    plan.pid = None;
    let _ = backfill::save(workspace, &plan);
    plan
}

/// One partition: an ordinary durable run with the slice's parameters bound.
fn run_one(
    workspace: &Path,
    duckdb: &Path,
    path: &Path,
    pipeline: &str,
    backfill_id: &str,
    slice: &PartitionRun,
    release: &Option<String>,
    gates: &crate::pools::Gates,
) -> Result<String, (Option<String>, String)> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| (None, format!("{}: {e}", path.display())))?;
    let doc: crate::PipelineDoc = serde_json::from_str(&text)
        .map_err(|e| (None, format!("{}: {e}", path.display())))?;
    run_doc(workspace, duckdb, doc, path, pipeline, backfill_id, slice, release, gates, "partition")
        .map(|(run_id, _rows)| run_id)
}

/// The one place a slice's run happens, whatever produced the document.
///
/// #306: a chunk builds a different document - one source, constrained by its
/// predicate, writing one part - but everything after that is the same run a
/// partition does: the pool, the receipt, the release, the parameter
/// provenance, the finish. Two functions here would be two answers to "was this
/// admitted", and the second one is always the one that forgets.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_doc(
    workspace: &Path,
    duckdb: &Path,
    mut doc: crate::PipelineDoc,
    // The pipeline this slice belongs to, for the receipt. A chunk runs a
    // document built here, but it belongs to the file the operator named.
    path: &Path,
    pipeline: &str,
    backfill_id: &str,
    slice: &PartitionRun,
    release: &Option<String>,
    gates: &crate::pools::Gates,
    // Where the slice's values came from, for #317's provenance: `partition`
    // or `chunk`.
    source: &str,
    // #306's ledger asks a slice to record rows as well as bytes, and the run
    // already counted them - returning them here beats a second pass over the
    // part to ask a question the run just answered.
) -> Result<(String, Option<u64>), (Option<String>, String)> {
    // Held for the whole slice, like any other run.
    let (_permit, pool, queued_ms) = gates.acquire(&doc.resource_pool);
    // #317's boundary, with `partition` as the source - so an operator reading
    // the receipt can see that a value came from the slice rather than from
    // them, and a partition value colliding with one they passed is recorded as
    // an override rather than silently winning.
    let supplied: Vec<crate::params::Supplied> = slice
        .params
        .iter()
        .map(|(name, value)| crate::params::Supplied {
            name: name.clone(),
            value: value.clone(),
            source: source.to_string(),
        })
        .collect();
    let (recorded, sources) =
        crate::context::apply_params_from(&mut doc, &supplied)
            .map_err(|e| (None, e))?;
    crate::context::apply_time_builtins(&mut doc);
    crate::context::apply_workspace_context(&mut doc, workspace);

    let hash = crate::retry::pipeline_hash(&doc);
    let run_id = crate::retry::new_run_id(pipeline, source);
    let receipt = crate::retry::begin(
        workspace,
        &run_id,
        source,
        pipeline,
        &path.display().to_string(),
        &hash,
        // The backfill is what caused this run, and naming it is what makes
        // "which slice produced this output" answerable from the receipt alone.
        Some(backfill_id.to_string()),
    );
    let receipt = crate::retry::RunReceipt {
        parameters: recorded,
        parameter_sources: sources,
        partition_key: Some(slice.key.clone()),
        resource_pool: Some(pool),
        queue_ms: Some(queued_ms),
        components: crate::plugin::used_by(
            workspace,
            &serde_json::to_value(&doc).unwrap_or_default(),
        ),
        release_id: release.clone().or(receipt.release_id.clone()),
        ..receipt
    };
    let _ = crate::retry::write(workspace, &receipt);

    let engine = crate::DuckdbEngine::new(duckdb.to_path_buf())
        .without_previews()
        .with_run_id(&run_id);
    let result = engine.execute_pipeline_named(&doc, pipeline);
    let status = result.status.clone();
    let error = result.error.clone();
    crate::retry::finish(
        workspace,
        receipt,
        &status,
        crate::retry::nodes_of(&result),
    );
    match status == "ok" {
        // The largest count any node reported: for a chunk that is the part
        // that was written, and for a document with one relation there is only
        // one number to choose from anyway.
        true => Ok((run_id, result.nodes.values().filter_map(|n| n.rows).max())),
        false => Err((Some(run_id), error.unwrap_or_else(|| "the run failed".into()))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backfill::{Kind, SliceArtifact};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn slices(keys: &[&str]) -> Backfill {
        Backfill {
            id: "bf-test".into(),
            pipeline: "extract".into(),
            pipeline_path: "pipelines/extract.json".into(),
            created_at: "2026-09-02T00:00:00Z".into(),
            release_id: Some("rel-1".into()),
            max_concurrent: 3,
            pid: None,
            kind: Kind::Chunk,
            chunk_node: Some("pg".into()),
            staging: None,
            epoch: None,
            partitions: keys
                .iter()
                .map(|k| PartitionRun {
                    key: (*k).to_string(),
                    state: State::Requested,
                    run_id: None,
                    attempts: 0,
                    error: None,
                    finished_at: None,
                    params: Default::default(),
                    occurrence: None,
                    predicate: Some(format!("id = '{k}'")),
                    artifact: None,
                    requires: None,
                    source_uri: None,
                })
                .collect(),
        }
    }

    /// A stand-in for the real work, so the LIFECYCLE can be tested without a
    /// database: what it returns is the only variable.
    struct Fake {
        ran: AtomicUsize,
        artifact: bool,
        requires: bool,
        fail: Option<String>,
    }

    impl SliceWork for Fake {
        fn run(&self, slice: &PartitionRun) -> Result<Done, (Option<String>, String)> {
            self.ran.fetch_add(1, Ordering::SeqCst);
            if let Some(k) = &self.fail {
                if k == &slice.key {
                    return Err((Some("run-x".into()), "the read failed".into()));
                }
            }
            Ok(Done {
                run_id: format!("run-{}", slice.key),
                artifact: self.artifact.then(|| SliceArtifact {
                    uri: format!("parts/{}.parquet", slice.key),
                    hash: "abc".into(),
                    bytes: 10,
                    rows: Some(1),
                }),
            })
        }
        fn requires_artifact(&self) -> bool {
            self.requires
        }
    }

    fn work(artifact: bool, requires: bool, fail: Option<&str>) -> Fake {
        Fake {
            ran: AtomicUsize::new(0),
            artifact,
            requires,
            fail: fail.map(str::to_string),
        }
    }

    /// #306, the rule Louis named: "the query completed" is not "the slice
    /// succeeded". Without this, a process dying between the read and the
    /// commit leaves a slice marked done whose output is not there, and the
    /// retry that exists to fix that skips it.
    #[test]
    fn a_slice_that_committed_nothing_is_not_succeeded() {
        let tmp = tempfile::tempdir().unwrap();
        let w = work(false, true, None);
        let out = execute_with(tmp.path(), slices(&["a", "b"]), true, &w, &|_| {});
        assert!(
            out.partitions.iter().all(|p| p.state == State::Failed),
            "a slice with no committed output was called succeeded: {:?}",
            out.counts()
        );
        assert!(
            out.partitions[0].error.as_deref().unwrap_or("").contains("no output was committed"),
            "the reason does not say what was missing: {:?}",
            out.partitions[0].error
        );
    }

    /// And the same work that DOES commit succeeds and keeps what it committed.
    #[test]
    fn a_committed_output_is_recorded_on_the_slice() {
        let tmp = tempfile::tempdir().unwrap();
        let w = work(true, true, None);
        let out = execute_with(tmp.path(), slices(&["a", "b"]), true, &w, &|_| {});
        assert!(out.is_done(), "{:?}", out.counts());
        assert_eq!(
            out.partitions[0].artifact.as_ref().map(|a| a.uri.clone()),
            Some("parts/a.parquet".to_string())
        );
    }

    /// An ordered chain: each link requires the one before (#326).
    fn ordered_chain(keys: &[&str]) -> Backfill {
        let mut b = slices(keys);
        b.kind = Kind::Sequence;
        b.chunk_node = None;
        for i in 1..b.partitions.len() {
            b.partitions[i].requires = Some(b.partitions[i - 1].key.clone());
        }
        b
    }

    /// Work that records the ORDER it was asked to do things in.
    struct Ordered {
        seen: Arc<Mutex<Vec<String>>>,
    }

    impl SliceWork for Ordered {
        fn run(&self, slice: &PartitionRun) -> Result<Done, (Option<String>, String)> {
            // Deliberately BACKWARDS: the earliest link sleeps longest. Run
            // concurrently, the order recorded would be the reverse of the
            // chain, so this test cannot pass by luck the way an equal sleep
            // would - three workers each taking one slice happened to record
            // a, b, c even with the predicate removed.
            let ms = 40u64.saturating_sub(20 * (slice.key.as_bytes()[0] - b'a') as u64);
            std::thread::sleep(std::time::Duration::from_millis(ms));
            self.seen.lock().unwrap_or_else(|p| p.into_inner()).push(slice.key.clone());
            Ok(Done { run_id: format!("run-{}", slice.key), artifact: None })
        }
        fn requires_artifact(&self) -> bool {
            false
        }
    }

    /// #326 acceptance criterion 3, through the real executor.
    ///
    /// `max_concurrent` is 3 here. An unordered plan of three slices would run
    /// them in parallel and in any order; this one must not, and the constraint
    /// comes from the claim predicate rather than from a second executor.
    #[test]
    fn an_ordered_chain_is_applied_in_order() {
        let tmp = tempfile::tempdir().unwrap();
        let w = Ordered { seen: Default::default() };
        let seen = Arc::clone(&w.seen);
        let out = execute_with(tmp.path(), ordered_chain(&["a", "b", "c"]), true, &w, &|_| {});
        assert!(out.is_done(), "{:?}", out.counts());
        assert_eq!(
            *seen.lock().unwrap(),
            ["a", "b", "c"],
            "a delta chain is serial by construction, not by luck"
        );
    }

    /// A hole stops the chain instead of being stepped over, and what is left
    /// is `requested` - blocked, not failed - so the file arriving later and a
    /// re-plan picks it straight back up.
    #[test]
    fn a_chain_stops_at_a_hole_rather_than_stepping_over_it() {
        let tmp = tempfile::tempdir().unwrap();
        let mut plan = ordered_chain(&["a", "c"]);
        // `c` requires `b`, which the publisher never released, so there is no
        // slice for it at all.
        plan.partitions[1].requires = Some("b".into());
        let w = work(false, false, None);
        let out = execute_with(tmp.path(), plan, true, &w, &|_| {});
        assert_eq!(out.partitions[0].state, State::Succeeded);
        assert_eq!(out.partitions[1].state, State::Requested, "c must not have run");
        assert_eq!(w.ran.load(Ordering::SeqCst), 1, "exactly one slice was worked");
        assert!(!out.is_done(), "a blocked chain has not finished");
        assert_eq!(
            out.blocked_reason(1).as_deref(),
            Some("waiting for b, which was never published")
        );
    }

    /// The regression that made a five-day backfill never terminate: a failed
    /// slice claimed by the same pass, forever. It applies to chunks the moment
    /// they share this loop, which is the point of sharing it.
    #[test]
    fn a_failed_slice_is_not_claimed_again_by_the_same_pass() {
        let tmp = tempfile::tempdir().unwrap();
        let w = work(true, true, Some("b"));
        let out = execute_with(tmp.path(), slices(&["a", "b", "c"]), true, &w, &|_| {});
        assert_eq!(w.ran.load(Ordering::SeqCst), 3, "a slice ran more than once");
        assert_eq!(out.counts().get("failed"), Some(&1), "{:?}", out.counts());
        assert_eq!(out.counts().get("succeeded"), Some(&2), "{:?}", out.counts());
    }

    /// #306: the one place the two kinds genuinely differ.
    ///
    /// A chunk ledger sent down the partition path would run the WHOLE pipeline
    /// once per chunk with no predicate, write the same output N times, and
    /// report every slice as succeeded. Five callers each remembering to
    /// dispatch is five chances to forget, so it is dispatched once.
    #[test]
    fn a_chunk_ledger_is_not_run_as_a_partitioned_backfill() {
        let tmp = tempfile::tempdir().unwrap();
        let pipeline = tmp.path().join("extract.json");
        std::fs::write(&pipeline, r#"{"nodes":[],"edges":[]}"#).unwrap();
        let mut plan = slices(&["a"]);
        plan.pipeline_path = pipeline.display().to_string();
        // Only the chunk path reads this, so only the chunk path can object.
        plan.chunk_node = None;

        let e = execute_ledger(tmp.path(), Path::new("duckdb"), plan, true, &|_| {})
            .expect_err("a chunk ledger was run as a partitioned backfill");
        assert!(e.contains("names no source node"), "{e}");
    }

    /// #295 + #306: reuse is reuse of the OUTPUT, not just of the run id. A
    /// chunk whose part has been deleted must be redone rather than adopted.
    #[test]
    fn a_reusable_slice_whose_output_is_gone_is_not_reused() {
        let tmp = tempfile::tempdir().unwrap();
        let occ = "occ-1".to_string();
        let mut prior = slices(&["a"]);
        prior.id = "bf-earlier".into();
        prior.partitions[0].state = State::Succeeded;
        prior.partitions[0].occurrence = Some(occ.clone());
        prior.partitions[0].run_id = Some("run-earlier".into());
        prior.partitions[0].artifact = Some(SliceArtifact {
            uri: tmp.path().join("gone.parquet").display().to_string(),
            hash: "abc".into(),
            bytes: 10,
            rows: None,
        });
        backfill::save(tmp.path(), &prior).unwrap();

        let mut next = slices(&["a"]);
        next.id = "bf-later".into();
        next.partitions[0].occurrence = Some(occ);
        let w = work(true, true, None);
        let out = execute_with(tmp.path(), next, false, &w, &|_| {});

        assert_eq!(w.ran.load(Ordering::SeqCst), 1, "the slice was adopted instead of redone");
        assert_eq!(out.partitions[0].run_id.as_deref(), Some("run-a"));
    }

    /// #295: a backfill slice is admitted by the machine's pool, not only by
    /// the backfill's own bound.
    ///
    /// Reads the source, because the failure is an executor that stops
    /// acquiring: `--max-concurrent 4` over a pipeline in a pool of one must
    /// still run one at a time, and a backfill is the most likely thing to try
    /// to get around that.
    ///
    /// The test lives here rather than in the CLI because the executor does -
    /// it moved when the console and MCP needed the same one, and a guard left
    /// behind in the old file would have gone on passing about nothing.
    #[test]
    fn every_partition_run_acquires_a_pool() {
        let src = include_str!("backfill_exec.rs");
        // Built from pieces so the needle does not appear here as a literal
        // and match itself, which it did once and made this pass against an
        // executor that had stopped acquiring anything.
        let needle = format!("gates.{}(&doc.resource_pool)", "acquire");
        let bound = format!("let (_permit, pool, queued_ms) = gates.{}", "acquire");
        let call = src
            .lines()
            .map(str::trim_start)
            .filter(|l| l.starts_with("let "))
            .find(|l| l.contains(&needle))
            .unwrap_or_else(|| {
                panic!("run_one no longer acquires a pool, so a backfill can outrun the machine")
            });
        // Bound for the life of the slice: a permit dropped at once admits
        // everything while still looking like it acquired.
        assert!(call.starts_with(&bound), "the permit is not held for the slice: {call}");
    }
}
