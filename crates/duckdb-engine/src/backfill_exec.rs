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
    let workers = plan
        .max_concurrent
        .min(plan.partitions.iter().filter(|p| p.state.is_claimable()).count())
        .max(1);
    // #295: a backfill's own bound is an ADDITIONAL ceiling, not a way around
    // the machine's. Each slice still acquires the pool its pipeline asks for,
    // so `--max-concurrent 4` over a pipeline in a pool of 1 runs one at a
    // time - the protection exists to stop two heavy jobs competing, and a
    // backfill is the most likely thing to try.
    let gates = std::sync::Arc::new(crate::pools::Gates::load(workspace));
    let id = plan.id.clone();
    let path = PathBuf::from(&plan.pipeline_path);
    let pipeline = plan.pipeline.clone();
    let release = plan.release_id.clone();
    let shared = Arc::new(Mutex::new(plan));
    let duckdb = duckdb.to_path_buf();

    std::thread::scope(|scope| {
        for _ in 0..workers {
            let shared = Arc::clone(&shared);
            let workspace = workspace.to_path_buf();
            let duckdb = duckdb.clone();
            let path = path.clone();
            let pipeline = pipeline.clone();
            let release = release.clone();
            let id = id.clone();
            let gates = std::sync::Arc::clone(&gates);
            scope.spawn(move || loop {
                // Claim one slice under the lock and mark it running on disk
                // before starting, so a crash leaves it `running` for the next
                // start to reconcile rather than looking untouched.
                let claimed = {
                    let mut plan = shared.lock().unwrap_or_else(|p| p.into_inner());
                    let Some(idx) = plan.partitions.iter().position(|p| p.state.is_claimable())
                    else {
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
                if let Some((other, run_id)) = done_already {
                    let mut plan = shared.lock().unwrap_or_else(|p| p.into_inner());
                    let p = &mut plan.partitions[idx];
                    p.state = State::Succeeded;
                    p.run_id = Some(run_id.clone());
                    p.finished_at = Some(chrono::Utc::now().to_rfc3339());
                    p.error = None;
                    let told = SliceOutcome {
                        key: slice.key.clone(),
                        run_id: Some(run_id),
                        error: None,
                        reused_from: Some(other),
                    };
                    let _ = backfill::save(&workspace, &plan);
                    drop(plan);
                    on_slice(told);
                    continue;
                }
                let outcome =
                    run_one(&workspace, &duckdb, &path, &pipeline, &id, &slice, &release, &gates);
                {
                    let mut plan = shared.lock().unwrap_or_else(|p| p.into_inner());
                    let p = &mut plan.partitions[idx];
                    p.finished_at = Some(chrono::Utc::now().to_rfc3339());
                    match &outcome {
                        Ok(run_id) => {
                            p.state = State::Succeeded;
                            p.run_id = Some(run_id.clone());
                            p.error = None;
                        }
                        Err((run_id, e)) => {
                            p.state = State::Failed;
                            p.run_id = run_id.clone();
                            p.error = Some(e.clone());
                        }
                    }
                    // Copied out before the save, so the callback does not
                    // hold a borrow of the plan while it is written.
                    let told = SliceOutcome {
                        key: slice.key.clone(),
                        run_id: p.run_id.clone(),
                        error: p.error.clone(),
                        reused_from: None,
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
    let mut doc: crate::PipelineDoc = serde_json::from_str(&text)
        .map_err(|e| (None, format!("{}: {e}", path.display())))?;
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
            source: "partition".to_string(),
        })
        .collect();
    let (recorded, sources) =
        crate::context::apply_params_from(&mut doc, &supplied)
            .map_err(|e| (None, e))?;
    crate::context::apply_time_builtins(&mut doc);
    crate::context::apply_workspace_context(&mut doc, workspace);

    let hash = crate::retry::pipeline_hash(&doc);
    let run_id = crate::retry::new_run_id(pipeline, "backfill");
    let receipt = crate::retry::begin(
        workspace,
        &run_id,
        "backfill",
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
        true => Ok(run_id),
        false => Err((Some(run_id), error.unwrap_or_else(|| "the run failed".into()))),
    }
}

#[cfg(test)]
mod tests {
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
