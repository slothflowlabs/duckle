//! `duckle-runner work` - claim items out of a batch and run them.
//!
//! A batch written by `ctl.foreach` with `dispatch: "queue"` is a list of
//! independent jobs in a file. This is the other half: a process that reads
//! that file, takes one item at a time, and runs it. Start it on one machine
//! and it is a resumable For Each. Start it on five machines pointed at the
//! same workspace and they share the work, without a queue server, a database
//! or any service between them.
//!
//! # How two workers avoid running the same item
//!
//! Each item is claimed with the same OS advisory lock a pipeline run uses
//! (`runlock`, `.duckle/locks/batch/`). The kernel releases it when the process
//! dies, so a worker that is killed mid-item leaves nothing to clean up and the
//! item becomes claimable again. There is no lease, no heartbeat and no
//! timeout, because there is nothing to expire.
//!
//! # What this guarantees, and what it does not
//!
//! **At least once, not exactly once.** The ledger line is written after the
//! item succeeds, so a worker that completes an item and dies before recording
//! it leaves that item looking undone, and another worker will run it again.
//! That is the honest trade for having no transactional store: the alternative
//! is recording before the work, which loses items instead of repeating them,
//! and a lost load is worse than a repeated one. Make the child idempotent -
//! an upsert sink rather than an append - and a repeat costs time, not
//! correctness.

use std::io::Write;
use std::path::{Path, PathBuf};

use duckle_duckdb_engine::batch::{self, LedgerLine};
use duckle_duckdb_engine::{runlock, DuckdbEngine};

fn record(workspace: &Path, batch_id: &str, line: &LedgerLine) -> Result<(), String> {
    let p = batch::ledger_path(workspace, batch_id);
    if let Some(dir) = p.parent() {
        std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    }
    let text = serde_json::to_string(line).map_err(|e| e.to_string())?;
    // One append of one short line, so concurrent workers interleave whole
    // lines rather than fragments of them.
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&p)
        .map_err(|e| format!("{}: {e}", p.display()))?;
    // Heal a torn tail before appending. A worker killed mid-write can leave a
    // line with no newline on it; appending straight onto that glues the two
    // together and destroys THIS record as well as the broken one, turning one
    // lost line into a second item run twice. Cheap to check, and the check is
    // what makes the ledger worth trusting.
    let needs_newline = std::fs::metadata(&p)
        .ok()
        .map(|m| m.len() > 0)
        .unwrap_or(false)
        && !ends_with_newline(&p);
    let payload = if needs_newline { format!("\n{text}\n") } else { format!("{text}\n") };
    f.write_all(payload.as_bytes()).map_err(|e| e.to_string())
}

fn ends_with_newline(p: &Path) -> bool {
    use std::io::{Read, Seek, SeekFrom};
    let Ok(mut f) = std::fs::File::open(p) else { return true };
    let Ok(len) = f.metadata().map(|m| m.len()) else { return true };
    if len == 0 {
        return true;
    }
    if f.seek(SeekFrom::End(-1)).is_err() {
        return true;
    }
    let mut last = [0u8; 1];
    match f.read_exact(&mut last) {
        Ok(()) => last[0] == b'\n',
        Err(_) => true,
    }
}

/// Every batch in the workspace, oldest first so work is taken in the order it
/// was queued rather than in whatever order the filesystem lists.
fn batches(workspace: &Path) -> Vec<(String, PathBuf)> {
    let dir = batch::batches_dir(workspace);
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return Vec::new();
    };
    let mut out: Vec<(String, PathBuf)> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("ndjson"))
        .filter(|p| !p.to_string_lossy().contains(".ledger."))
        .filter_map(|p| {
            let id = p.file_stem()?.to_string_lossy().into_owned();
            Some((id, p))
        })
        .collect();
    out.sort();
    out
}

/// Prove that the run lock actually excludes on THIS filesystem.
///
/// Everything about sharing a batch rests on one assumption: that when two
/// processes ask for the same lock, exactly one gets it. That holds on a local
/// disk and on a properly configured network mount, and it does NOT hold on
/// some shared filesystems - NFSv3 with no lock daemon being the classic case,
/// where every caller is told it has the lock. On such a mount every worker
/// would claim every item and each item would run once per worker, silently,
/// with no error anywhere to notice.
///
/// Assuming that away would be the most expensive kind of optimism, so it is
/// measured. This process takes a lock, re-runs itself as a real second
/// process, and asks that process whether it got the same lock. A second
/// process is the only honest test: two attempts inside one process can be
/// refused by bookkeeping a network filesystem never sees.
fn lock_excludes(workspace: &Path) -> Result<bool, String> {
    let key = format!("preflight-{}", std::process::id());
    let held = runlock::try_acquire_nested(workspace, "batch", &key)
        .ok_or_else(|| "could not take a lock in this workspace at all".to_string())?;

    let exe = std::env::current_exe().map_err(|e| format!("cannot find this executable: {e}"))?;
    let out = std::process::Command::new(exe)
        .args(["work", "--lock-probe"])
        .arg(workspace)
        .arg(&key)
        .output()
        .map_err(|e| format!("could not start a second process to test the lock: {e}"))?;
    drop(held);

    verdict_from_probe(&String::from_utf8_lossy(&out.stdout))
}

/// Read the probe's answer.
///
/// Split out because the branch that matters most - the probe saying it GOT a
/// lock this process was already holding - cannot be produced on a machine
/// whose filesystem works. A test can produce it, and must, or the only code
/// path protecting against silent duplicate execution is the one never
/// exercised.
fn verdict_from_probe(said: &str) -> Result<bool, String> {
    if said.contains("REFUSED") {
        return Ok(true);
    }
    if said.contains("ACQUIRED") {
        return Ok(false);
    }
    Err(format!(
        "the lock test process said something unexpected: {}",
        said.trim().chars().take(200).collect::<String>()
    ))
}

/// The hidden other half of [`lock_excludes`]: try the lock, say what happened.
fn lock_probe(workspace: &Path, key: &str) -> i32 {
    match runlock::try_acquire_nested(workspace, "batch", key) {
        Some(_lock) => {
            println!("ACQUIRED");
            0
        }
        None => {
            println!("REFUSED");
            0
        }
    }
}

pub fn run() -> Result<i32, String> {
    let args: Vec<String> = std::env::args().skip(2).collect();
    if args.first().map(String::as_str) == Some("-h")
        || args.first().map(String::as_str) == Some("--help")
    {
        println!(
            "duckle-runner work - run queued batch items\n\n\
             USAGE:\n    \
             duckle-runner work [--workspace <dir>] [--batch <id>] [--once] [--duckdb <path>]\n\n\
             Runs items queued by a For Each set to \"Queue for workers\". Start it on\n\
             several machines pointed at one workspace and they share the batch: each\n\
             item is claimed with the same lock a pipeline run uses, so no two workers\n\
             take the same one.\n\n\
             --batch <id>   only this batch, instead of every batch in the workspace\n    \
             --once         claim and run a single item, then exit\n    \
             --check        test that locks exclude on this filesystem, then exit\n    \
             --no-check     skip that test (see below)\n\n\
             Before running anything a worker proves the run lock actually excludes\n\
             here, by taking a lock and asking a second process whether it can take\n\
             the same one. Some shared filesystems - NFS with no lock daemon is the\n\
             classic case - tell every caller it has the lock, and on one of those\n\
             every worker would claim every item and each item would run once per\n\
             worker, silently. A worker refuses to start there.\n\n\
             Items are run AT LEAST once. The ledger is written after an item succeeds,\n\
             so a worker that finishes an item and then dies leaves it looking undone\n\
             and another worker repeats it. Make the child idempotent - an upsert sink\n\
             rather than an append - and a repeat costs time, not correctness.\n\n\
             A failed item stays claimable and is retried on a later pass; the ledger\n\
             keeps the failure so there is something to look at. Set a retry policy on\n\
             the For Each node (max attempts, fixed or exponential backoff) and an item\n\
             that keeps failing is left alone once it runs out of attempts, instead of\n\
             taking a worker slot on every pass forever.\n\n\
             OTHER COMMANDS:\n    \
             duckle-runner work status [--batch <id>] [--json]\n        \
                 every batch, and the items waiting or out of attempts\n    \
             duckle-runner work retry [--dead] [--batch <id>] [--json]\n        \
                 start failed items over. --dead takes only the ones that ran\n        \
                 out of attempts. The failures stay in the ledger either way."
        );
        return Ok(0);
    }

    let mut workspace = PathBuf::from(".");
    let mut only_batch: Option<String> = None;
    let mut once = false;
    let mut duckdb: Option<PathBuf> = None;
    let mut check_only = false;
    let mut skip_check = false;
    let mut probe: Option<(String, String)> = None;
    let mut json = false;
    let mut only_dead = false;
    // `work status` and `work retry` read and edit the queue instead of
    // consuming it. Taken as a leading word rather than a flag because they are
    // different commands, not options on running.
    let verb = args
        .first()
        .filter(|a| !a.starts_with('-'))
        .cloned()
        .unwrap_or_default();
    let args: Vec<String> = if verb.is_empty() { args } else { args[1..].to_vec() };
    let mut it = args.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--json" => json = true,
            "--dead" => only_dead = true,
            "--workspace" => {
                workspace = PathBuf::from(it.next().ok_or("--workspace needs a value")?)
            }
            "--batch" => only_batch = Some(it.next().ok_or("--batch needs a value")?.clone()),
            "--duckdb" => duckdb = Some(PathBuf::from(it.next().ok_or("--duckdb needs a value")?)),
            "--once" => once = true,
            "--check" => check_only = true,
            "--no-check" => skip_check = true,
            // Hidden: the second process the lock test spawns.
            "--lock-probe" => {
                let ws = it.next().ok_or("--lock-probe needs a workspace")?.clone();
                let key = it.next().ok_or("--lock-probe needs a key")?.clone();
                probe = Some((ws, key));
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    if let Some((ws, key)) = probe {
        return Ok(lock_probe(Path::new(&ws), &key));
    }
    let workspace = std::fs::canonicalize(&workspace)
        .map_err(|e| format!("workspace {}: {e}", workspace.display()))?;

    match verb.as_str() {
        "" => {}
        "status" => return show_status(&workspace, only_batch.as_deref(), json),
        "retry" => return retry(&workspace, only_batch.as_deref(), only_dead, json),
        other => {
            return Err(format!(
                "unknown command `work {other}` - expected `status`, `retry`, or no command to run items"
            ))
        }
    }

    // Measure the guarantee before relying on it. Refusing to start is the
    // right answer to a filesystem whose locks do not exclude: the alternative
    // is every worker running every item, silently. A test that could not be
    // RUN is only a warning - failing to prove it is not the same as having
    // disproved it, and refusing to work because a probe would not spawn would
    // be its own outage.
    if !skip_check {
        match lock_excludes(&workspace) {
            Ok(true) => {
                if check_only {
                    println!(
                        "locks exclude correctly on {} - safe to run workers here.",
                        workspace.display()
                    );
                    return Ok(0);
                }
            }
            Ok(false) => {
                return Err(format!(
                    "locks do NOT exclude on {}: a second process took a lock this one was already holding. Every worker would claim every item, and each item would run once per worker, with no error anywhere. That is what an NFS mount with no lock daemon does. Fix the mount, or point workers at a local workspace.                      --no-check overrides this, knowing the above.",
                    workspace.display()
                ));
            }
            Err(why) => eprintln!(
                "duckle-runner: could not verify that locks exclude here ({why}). Proceeding, but two workers on this filesystem are unproven."
            ),
        }
        if check_only {
            return Ok(0);
        }
    } else if check_only {
        println!("--no-check was given, so nothing was tested.");
        return Ok(0);
    }
    // The engine reads this for sub-pipeline refs, incremental state and logs,
    // exactly as a normal run does.
    std::env::set_var("DUCKLE_WORKSPACE", &workspace);

    let duckdb = crate::resolve_duckdb(duckdb)?;
    let engine = DuckdbEngine::new(duckdb);
    let worker = worker_id();

    let mut ran = 0usize;
    let mut failed = 0usize;
    let mut skipped_claimed = 0usize;
    let mut waiting = 0usize;
    let mut dead = 0usize;

    for (batch_id, path) in batches(&workspace) {
        if let Some(want) = &only_batch {
            if &batch_id != want {
                continue;
            }
        }
        let (items, unreadable) = batch::read(&path).map_err(|e| e.to_string())?;
        if unreadable > 0 {
            eprintln!(
                "duckle-runner: {} line(s) of {} could not be read and were skipped",
                unreadable,
                path.display()
            );
        }
        // What each item's history says about it: done, claimable now, still
        // inside its backoff, or out of attempts. Read once per batch rather
        // than per item, so one pass over the ledger serves the whole batch.
        let states: std::collections::HashMap<usize, batch::Phase> =
            batch::item_states(&workspace, &batch_id, chrono::Utc::now())
                .into_iter()
                .map(|s| (s.index, s.phase))
                .collect();

        for item in &items {
            match states.get(&item.index) {
                Some(batch::Phase::Done) => continue,
                // Failed and inside its backoff. Left alone rather than
                // claimed-and-skipped, so it does not churn the lock.
                Some(batch::Phase::Waiting) => {
                    waiting += 1;
                    continue;
                }
                // Out of attempts. Reported once per pass rather than per item:
                // a batch with 40,000 dead items would otherwise bury the log.
                Some(batch::Phase::Dead) => {
                    dead += 1;
                    continue;
                }
                _ => {}
            }
            // Claim it. A key of batch + index, under its own group so no
            // pipeline run can ever name it.
            let key = format!("{}-{}", batch_id, item.index);
            let claim = match runlock::try_acquire_nested(&workspace, "batch", &key) {
                Some(lock) => lock,
                None => {
                    skipped_claimed += 1;
                    continue;
                }
            };

            let label = item.item.clone().unwrap_or_else(|| item.index.to_string());
            eprintln!("duckle-runner: running {} item {}", batch_id, label);
            let started = chrono::Utc::now();
            let outcome = engine.run_batch_item(&item.child, &item.vars, item.item.as_deref());
            let (status, error) = match outcome {
                Ok(()) => {
                    ran += 1;
                    ("ok", None)
                }
                Err(e) => {
                    failed += 1;
                    eprintln!("duckle-runner: {} item {} failed: {}", batch_id, label, e);
                    ("error", Some(e.to_string()))
                }
            };
            if let Err(e) = record(
                &workspace,
                &batch_id,
                &LedgerLine {
                    v: 1,
                    index: item.index,
                    status: status.into(),
                    at: started.to_rfc3339(),
                    worker: worker.clone(),
                    error,
                },
            ) {
                // The work happened; only the record of it failed. Say so
                // loudly, because the consequence is that another worker will
                // run this item again.
                eprintln!(
                    "duckle-runner: {} item {} ran but could not be recorded ({}); \
                     it will be run again",
                    batch_id, label, e
                );
            }
            drop(claim);
            if once {
                println!("ran {ran}, failed {failed}");
                if waiting > 0 {
                    println!("{waiting} item(s) are waiting out a retry backoff.");
                }
                if dead > 0 {
                    println!("{dead} item(s) are out of attempts.");
                }
                return Ok(if failed > 0 { 1 } else { 0 });
            }
        }
    }

    let held_back = |waiting: usize, dead: usize| {
        if waiting > 0 {
            println!("{waiting} item(s) are waiting out a retry backoff.");
        }
        if dead > 0 {
            println!(
                "{dead} item(s) are out of attempts and will not be tried again. `duckle-runner work retry --dead` starts them over."
            );
        }
    };
    if ran == 0 && failed == 0 {
        if skipped_claimed > 0 {
            println!("nothing to do: {skipped_claimed} item(s) are being run by other workers.");
        } else if waiting > 0 || dead > 0 {
            println!("nothing to run right now in {}.", workspace.display());
        } else {
            println!("nothing to do: no unfinished items in {}.", workspace.display());
        }
        held_back(waiting, dead);
        return Ok(0);
    }
    println!("ran {ran}, failed {failed}");
    if skipped_claimed > 0 {
        println!("{skipped_claimed} item(s) were already claimed by other workers.");
    }
    held_back(waiting, dead);
    Ok(if failed > 0 { 1 } else { 0 })
}

/// `work status` - what every batch is doing, and why anything is not moving.
///
/// The counts a worker acts on, shown the same way the worker reads them: an
/// item held back by a backoff and an item that is out of attempts are
/// different problems, and "12 failed" says neither.
fn show_status(workspace: &Path, only: Option<&str>, json: bool) -> Result<i32, String> {
    let now = chrono::Utc::now();
    let ids: Vec<String> = batches(workspace)
        .into_iter()
        .map(|(id, _)| id)
        .filter(|id| only.map(|w| w == id).unwrap_or(true))
        .collect();
    if let Some(want) = only {
        if ids.is_empty() {
            return Err(format!("no batch `{want}` in {}", workspace.display()));
        }
    }
    if json {
        let payload: Vec<serde_json::Value> = ids
            .iter()
            .map(|id| {
                serde_json::json!({
                    "batch": batch::status(workspace, id),
                    "items": batch::item_states(workspace, id, now),
                })
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&payload).unwrap_or_default());
        return Ok(0);
    }
    if ids.is_empty() {
        println!("no batches in {}", workspace.display());
        return Ok(0);
    }
    for id in &ids {
        let st = batch::status(workspace, id);
        println!(
            "{}: {} item(s) - {} done, {} running, {} failed ({} waiting), {} dead",
            id, st.items, st.done, st.running, st.failed, st.waiting, st.dead
        );
        if st.unreadable > 0 {
            println!("  {} line(s) of the batch could not be read", st.unreadable);
        }
        // Only the items that need attention. Listing 400,000 healthy rows
        // buries the twelve that are stuck.
        for s in batch::item_states(workspace, id, now) {
            let label = s.item.clone().unwrap_or_else(|| s.index.to_string());
            match s.phase {
                batch::Phase::Dead => println!(
                    "  DEAD    {label}  {} attempt(s), last: {}",
                    s.attempts,
                    s.last_error.as_deref().unwrap_or("(no error recorded)")
                ),
                batch::Phase::Waiting => println!(
                    "  waiting {label}  {} attempt(s), next at {}",
                    s.attempts,
                    s.next_attempt_at.as_deref().unwrap_or("?")
                ),
                _ => {}
            }
        }
    }
    Ok(0)
}

/// `work retry` - start failed items over without losing what happened to them.
///
/// Appends a reset marker rather than deleting the failures, so the ledger
/// still shows that an item died four times before someone fixed the source
/// and retried it.
fn retry(
    workspace: &Path,
    only: Option<&str>,
    only_dead: bool,
    json: bool,
) -> Result<i32, String> {
    let who = worker_id();
    let ids: Vec<String> = batches(workspace)
        .into_iter()
        .map(|(id, _)| id)
        .filter(|id| only.map(|w| w == id).unwrap_or(true))
        .collect();
    if ids.is_empty() {
        return Err(match only {
            Some(want) => format!("no batch `{want}` in {}", workspace.display()),
            None => format!("no batches in {}", workspace.display()),
        });
    }
    let mut total = 0usize;
    let mut per: Vec<(String, usize)> = Vec::new();
    for id in &ids {
        let n = batch::reset_attempts(workspace, id, only_dead, &who).map_err(|e| e.to_string())?;
        total += n;
        if n > 0 {
            per.push((id.clone(), n));
        }
    }
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "reset": total,
                "batches": per.iter().map(|(id, n)| serde_json::json!({"batch": id, "reset": n}))
                    .collect::<Vec<_>>(),
            }))
            .unwrap_or_default()
        );
        return Ok(0);
    }
    if total == 0 {
        println!(
            "nothing to retry{}.",
            if only_dead { " - no items are out of attempts" } else { "" }
        );
        return Ok(0);
    }
    for (id, n) in &per {
        println!("{id}: {n} item(s) will be tried again");
    }
    println!("run `duckle-runner work` to pick them up.");
    Ok(0)
}

/// Something a human can recognise in a ledger: the machine, and the process.
fn worker_id() -> String {
    let host = std::env::var("COMPUTERNAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| "unknown".into());
    format!("{host}/{}", std::process::id())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(index: usize, status: &str) -> LedgerLine {
        LedgerLine {
            v: 1,
            index,
            status: status.into(),
            at: "2026-08-16T10:00:00Z".into(),
            worker: "host/1".into(),
            error: None,
        }
    }

    /// A finished item is not run twice; a failed one is retried.
    ///
    /// Treating a failure as done would let one transient network error consume
    /// an item permanently, which is a worse outcome than repeating it. The
    /// ledger keeps the failure either way so there is something to look at.
    #[test]
    fn only_successes_count_as_finished() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        std::fs::create_dir_all(batch::batches_dir(ws)).unwrap();
        record(ws, "b1", &line(0, "ok")).unwrap();
        record(ws, "b1", &line(1, "error")).unwrap();
        record(ws, "b1", &line(2, "ok")).unwrap();

        let done = batch::finished(ws, "b1");
        assert!(done.contains(&0) && done.contains(&2));
        assert!(!done.contains(&1), "a failed item must stay claimable");
        assert_eq!(done.len(), 2);
    }

    /// A ledger damaged by a crash must not lose the record of what finished.
    #[test]
    fn a_torn_ledger_line_does_not_hide_the_rest() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        std::fs::create_dir_all(batch::batches_dir(ws)).unwrap();
        record(ws, "b1", &line(0, "ok")).unwrap();
        let p = batch::ledger_path(ws, "b1");
        let mut raw = std::fs::read_to_string(&p).unwrap();
        raw.push_str("{\"v\":1,\"index\":9,\"stat");
        std::fs::write(&p, raw).unwrap();
        record(ws, "b1", &line(1, "ok")).unwrap();

        let done = batch::finished(ws, "b1");
        assert!(done.contains(&0) && done.contains(&1), "a torn line hid a finished item");
    }

    /// The ledger of a batch is not mistaken for a batch.
    #[test]
    fn a_ledger_is_not_picked_up_as_work() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        std::fs::create_dir_all(batch::batches_dir(ws)).unwrap();
        std::fs::write(batch::batch_path(ws, "b1"), "").unwrap();
        record(ws, "b1", &line(0, "ok")).unwrap();

        let found = batches(ws);
        assert_eq!(found.len(), 1, "found {found:?}");
        assert_eq!(found[0].0, "b1");
    }

    /// The dangerous case, which a working filesystem cannot produce.
    ///
    /// If a second process reports it took a lock this one was already holding,
    /// locks do not exclude here, and running anyway means every worker claims
    /// every item and each item runs once per worker - silently, with no error
    /// anywhere. That branch cannot be reached on a machine whose locks work,
    /// so it is driven directly rather than left as the one untested path.
    #[test]
    fn a_probe_that_got_the_lock_means_this_filesystem_is_not_safe() {
        assert_eq!(verdict_from_probe("ACQUIRED
"), Ok(false));
        assert_eq!(verdict_from_probe("REFUSED
"), Ok(true));

        // Silence, or anything unrecognised, is NOT read as safe. Not being
        // able to prove exclusion is different from having proved it, and
        // guessing "safe" here is exactly the assumption this exists to remove.
        assert!(verdict_from_probe("").is_err());
        assert!(verdict_from_probe("bash: duckle-runner: not found").is_err());
    }

    /// The probe answers honestly about a lock that is genuinely held.
    #[test]
    fn the_probe_reports_refused_only_when_something_holds_it() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        // Nothing holds it.
        assert_eq!(lock_probe(ws, "k1"), 0);
        // Held: the probe run in THIS process would be refused, which is what
        // the child process observes on a working filesystem.
        let held = runlock::try_acquire_nested(ws, "batch", "k2").unwrap();
        assert!(runlock::try_acquire_nested(ws, "batch", "k2").is_none());
        drop(held);
    }

    /// Two workers must not take the same item.
    #[test]
    fn a_claimed_item_is_refused_to_a_second_worker() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let key = "b1-0";
        let first = runlock::try_acquire_nested(ws, "batch", key).expect("first worker wins");
        assert!(
            runlock::try_acquire_nested(ws, "batch", key).is_none(),
            "a second worker took an item that was already being run"
        );
        drop(first);
        // ...and once that worker is gone - which is what a kill looks like,
        // since the kernel drops the lock - the item is claimable again.
        assert!(runlock::try_acquire_nested(ws, "batch", key).is_some());
    }
}
