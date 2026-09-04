//! A cross-process lock for one scheduled pipeline run.
//!
//! Duckle has two schedulers: the desktop app runs one inside the Tauri
//! process, and `duckle-runner serve` runs another. Both guard themselves in
//! process - a semaphore in one, a condvar gate in the other - and neither
//! knows the other exists. Point both at the same workspace, which is exactly
//! what happens while promoting a workspace from a laptop to a server, and the
//! same schedule fires twice at the same instant: two runs writing the same
//! sink, and two runs advancing the same `xf.incremental` watermark, which is
//! how a load silently skips rows.
//!
//! The lock is the operating system's, not ours. On Windows the file is opened
//! with no sharing, so a second opener is refused; on Unix the descriptor takes
//! a non-blocking `flock`. Both are released by the kernel when the handle
//! closes, which includes the process being killed or crashing - so a run that
//! dies mid-flight cannot wedge a schedule forever, and there is no stale-lock
//! timeout to tune or get wrong.
//!
//! Acquisition never waits. A schedule that is already running elsewhere is
//! skipped for this tick rather than queued, because the next tick will come
//! around anyway and a queue of identical overdue runs is not useful.

use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};

/// Held for the duration of a run. Dropping it releases the lock.
#[derive(Debug)]
pub struct RunLock {
    /// Releasing happens when this closes; nothing else is required.
    _file: File,
    key: String,
}

impl RunLock {
    /// Which run this lock covers, for logging.
    pub fn key(&self) -> &str {
        &self.key
    }
}

/// A stable 64-bit digest of a key.
///
/// FNV-1a, written out rather than taken from the standard library: the output
/// of `DefaultHasher` is explicitly not guaranteed across Rust releases, and
/// two Duckle builds sharing one workspace - a desktop app on one version and
/// a runner on another, which is the normal state mid-upgrade - must derive the
/// same lock filename or neither of them is locking anything.
fn digest(key: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in key.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Reduce a caller-supplied name to something safe to use as a filename.
///
/// Keys are pipeline ids that reach us from a file on disk, so they are
/// sanitised rather than trusted: anything that is not plainly a name becomes
/// an underscore, which also flattens any path separator.
///
/// The digest is what makes the result unique, and it is not decoration.
/// Sanitising alone is many-to-one: `sales.daily` and `sales_daily` both became
/// `sales_daily`, and every non-ASCII name of the same length became the same
/// run of underscores, so two unrelated pipelines shared one lock. The
/// symptom was the worst kind - a scheduled run silently skipped, with a log
/// line blaming a different pipeline that happened to be running.
fn safe_name(key: &str) -> String {
    // Enough of the original to recognise in a directory listing; the digest
    // carries the identity.
    let stem: String = key
        .chars()
        .take(48)
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect();
    format!("{stem}-{:016x}", digest(key))
}

/// Where a lock for `key` lives. Kept beside the workspace key material, under
/// `.duckle`, so everything the runtime owns is in one place.
fn lock_path(workspace: &Path, key: &str) -> PathBuf {
    workspace.join(".duckle").join("locks").join(format!("{}.lock", safe_name(key)))
}

/// Where a lock one level down, under `group`, lives.
fn nested_lock_path(workspace: &Path, group: &str, key: &str) -> PathBuf {
    workspace
        .join(".duckle")
        .join("locks")
        .join(safe_name(group))
        .join(format!("{}.lock", safe_name(key)))
}

#[cfg(windows)]
fn open_exclusive(path: &Path) -> std::io::Result<File> {
    use std::os::windows::fs::OpenOptionsExt;
    // share_mode(0) means no other handle may be opened while this one lives,
    // so a second process gets a sharing violation instead of a second lock.
    OpenOptions::new().create(true).write(true).share_mode(0).open(path)
}

#[cfg(unix)]
fn open_exclusive(path: &Path) -> std::io::Result<File> {
    use std::os::unix::io::AsRawFd;
    let file = OpenOptions::new().create(true).write(true).open(path)?;
    // LOCK_NB so this reports "someone else has it" rather than blocking the
    // scheduler tick behind a run that might take hours.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(file)
}

/// Take the lock for `key`, or return `None` when another process holds it.
///
/// A workspace that cannot be written - read-only mount, missing directory it
/// cannot create - yields `None` as well. That is deliberate: if the lock
/// cannot be taken, the run does not happen, because the failure mode of
/// running anyway is the duplicate this exists to prevent.
pub fn try_acquire(workspace: &Path, key: &str) -> Option<RunLock> {
    acquire_at(lock_path(workspace, key), key)
}

/// Take a lock that lives one level down, under `group`.
///
/// For locks that guard something other than a pipeline run and must never be
/// blocked by one. A run key cannot reach this path: separators in a key are
/// flattened to underscores, so no pipeline id can name a subdirectory.
/// Hold an exclusive lock on a named store, waiting briefly for whoever has it.
///
/// Unlike a run lock, where a clash means "someone else is already doing this,
/// so skip", a clash here means "someone else is mid-write, so wait": the write
/// takes a few milliseconds and the caller is holding a change that must not be
/// dropped. The ceiling turns a wedged holder into a reported error rather than
/// a hung UI; the kernel releases the lock on process death, so reaching it at
/// all should mean a genuinely stuck writer.
///
/// Every store that two processes share goes through this - schedules.json and
/// alert-state.json today. A store that reads, modifies and writes without it
/// loses whichever writer finished first.
pub fn lock_store(workspace: &Path, name: &str) -> Result<RunLock, String> {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        // A nested key: pipeline ids flatten their separators to underscores,
        // so no pipeline can ever name this lock and stall a save by running.
        match acquire_at_reason(nested_lock_path(workspace, "store", name), name) {
            AcquireOutcome::Claimed(lock) => return Ok(lock),
            // Waiting cannot fix a workspace that will not take a lock, and
            // spending five seconds to say "timed out waiting" hides the real
            // reason behind a message about somebody else.
            AcquireOutcome::Unusable(e) => {
                return Err(format!("cannot lock {name} in this workspace: {e}"))
            }
            AcquireOutcome::HeldByOther => {}
        }
        if std::time::Instant::now() >= deadline {
            return Err(format!("Timed out waiting to write {name}"));
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
}

/// Take a lock that lives one level down, under `group`.
///
/// For locks that guard something other than a pipeline run and must never be
/// blocked by one. A run key cannot reach this path: separators in a key are
/// flattened to underscores, so no pipeline id can name a subdirectory.
pub fn try_acquire_nested(workspace: &Path, group: &str, key: &str) -> Option<RunLock> {
    acquire_at(nested_lock_path(workspace, group, key), key)
}

fn acquire_at(path: PathBuf, key: &str) -> Option<RunLock> {
    match acquire_at_reason(path, key) {
        AcquireOutcome::Claimed(lock) => Some(lock),
        // Held elsewhere, or unwritable. Either way this process does not run.
        _ => None,
    }
}

/// Why a lock attempt ended the way it did.
///
/// `try_acquire` collapses every failure to `None`, which is right for the
/// caller that only has to decide whether to run. It is wrong for the caller
/// that has to tell an operator what happened: "another process is running
/// this" and "I cannot write to this workspace at all" call for completely
/// different actions, and reporting the first when the second is true sends
/// somebody hunting for a process that does not exist.
#[derive(Debug)]
pub enum AcquireOutcome {
    Claimed(RunLock),
    /// Another live process holds it. Coming back later will work.
    HeldByOther,
    /// This process cannot lock here at all: a read-only mount, a directory it
    /// may not create, a filesystem with no working locks. Coming back later
    /// changes nothing.
    Unusable(std::io::Error),
}

/// Whether a failure means "someone else holds it" rather than "this workspace
/// cannot be locked".
fn is_contention(e: &std::io::Error) -> bool {
    #[cfg(windows)]
    {
        // ERROR_SHARING_VIOLATION (32) / ERROR_LOCK_VIOLATION (33): the file is
        // already open elsewhere with share_mode(0). Anything else - access
        // denied, read-only, path not found - is this machine's problem rather
        // than another process's.
        matches!(e.raw_os_error(), Some(32) | Some(33))
    }
    #[cfg(unix)]
    {
        // flock(LOCK_NB) reports EWOULDBLOCK/EAGAIN when another open file
        // description holds the lock; the open itself failing is not that.
        e.kind() == std::io::ErrorKind::WouldBlock
    }
}

/// Like [`try_acquire`], but says why.
pub fn try_acquire_reason(workspace: &Path, key: &str) -> AcquireOutcome {
    acquire_at_reason(lock_path(workspace, key), key)
}

fn acquire_at_reason(path: PathBuf, key: &str) -> AcquireOutcome {
    if let Some(dir) = path.parent() {
        if let Err(e) = fs::create_dir_all(dir) {
            return AcquireOutcome::Unusable(e);
        }
    }
    match open_exclusive(&path) {
        Ok(file) => AcquireOutcome::Claimed(RunLock { _file: file, key: key.to_string() }),
        Err(e) if is_contention(&e) => AcquireOutcome::HeldByOther,
        Err(e) => AcquireOutcome::Unusable(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_second_acquire_is_refused_while_the_first_is_held() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();

        let first = try_acquire(ws, "nightly-load").expect("first caller wins");
        assert_eq!(first.key(), "nightly-load");
        assert!(
            try_acquire(ws, "nightly-load").is_none(),
            "two holders of the same run lock at once"
        );

        // A different schedule is unaffected - the lock is per run, not global,
        // so unrelated pipelines still fire on time.
        let other = try_acquire(ws, "hourly-sync").expect("different key is free");
        drop(other);

        // Releasing lets the next caller through, which is what makes the
        // schedule resume on the following tick rather than stalling.
        drop(first);
        // Asserted on the CLASSIFIED outcome rather than on `is_some()`.
        //
        // `try_acquire` collapses "another holder" and "this workspace cannot
        // be locked at all" into the same `None`, so when this failed on CI it
        // said only "lock never became available again" - which is the one
        // thing that could not have been true, since nothing held it. Three
        // reds and no way to tell contention from an `open` that failed for an
        // environmental reason, on a runner executing 1160 tests at once.
        //
        // HeldByOther is the real regression and still fails loudly. Unusable
        // is not what this test is about: the property is that RELEASING lets
        // the next caller through, not that a file can be opened right now.
        //
        // An Unusable outcome is retried briefly rather than failed on. It is
        // not the property under test, and it is transient by definition: a
        // read-only mount or a filesystem without locks stays broken for the
        // whole second, while an `open` that lost a race for a file descriptor
        // on a runner executing 1160 tests at once clears immediately. A
        // HeldByOther is never retried - that IS the regression, and retrying
        // it would hide exactly what this test exists to catch.
        let mut last = None;
        for _ in 0..50 {
            match try_acquire_reason(ws, "nightly-load") {
                AcquireOutcome::Claimed(_) => return,
                AcquireOutcome::HeldByOther => {
                    panic!("the lock was still held after its only holder was dropped")
                }
                AcquireOutcome::Unusable(e) => {
                    last = Some(e.to_string());
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
            }
        }
        panic!(
            "the workspace could not be locked at all for a second, so this is the              environment rather than the lock: {}",
            last.unwrap_or_default()
        );
    }

    /// The whole point of this module is holding across PROCESSES, so a
    /// same-process test proves the wrong thing. This one re-runs the test
    /// binary as a real child, has it take the lock, and checks that the
    /// parent is refused while the child lives. The two talk through marker
    /// files rather than a sleep, so a slow machine makes the test slower
    /// rather than flaky.
    #[test]
    fn a_second_os_process_is_refused_while_the_first_holds_the_lock() {
        use std::process::Command;
        use std::time::{Duration, Instant};

        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();

        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["runlock::tests::child_holds_the_lock", "--exact", "--nocapture"])
            .env(CHILD_ENV, ws)
            .spawn()
            .expect("could not re-run the test binary as a child");

        // Wait for the child to actually be holding it. Anything else - the
        // child failing to acquire, or exiting early - is a broken test rather
        // than a passing one, so both are reported instead of timing out.
        let held = ws.join("held");
        let deadline = Instant::now() + Duration::from_secs(30);
        while !held.exists() {
            if ws.join("failed").exists() {
                let _ = child.wait();
                panic!("the child could not take the lock, so nothing was proved");
            }
            if let Ok(Some(status)) = child.try_wait() {
                panic!("the child exited ({status}) before taking the lock");
            }
            if Instant::now() > deadline {
                let _ = child.kill();
                panic!("the child never reported holding the lock");
            }
            std::thread::sleep(Duration::from_millis(20));
        }

        // The measurement: another process holds it, so this one is refused.
        assert!(
            try_acquire(ws, "cross-process").is_none(),
            "two OS processes held the same run lock at once"
        );
        // ...and it is that run that is locked, not the workspace. A second
        // schedule must still be free to fire while the first one runs.
        assert!(
            try_acquire(ws, "some-other-run").is_some(),
            "one running schedule blocked an unrelated one"
        );

        fs::write(ws.join("release"), b"").unwrap();
        let status = child.wait().unwrap();
        assert!(status.success(), "child test failed: {status}");

        // The kernel dropped it when the child's handle closed, so the next
        // tick can run. This also covers the crash case: nothing but process
        // death is needed to release, so there is no stale lock to time out.
        assert!(
            try_acquire(ws, "cross-process").is_some(),
            "the lock survived the process that held it"
        );
    }

    /// Not a test of its own - the child half of the case above, which is why
    /// it does nothing at all unless the parent asked for it by env var.
    #[test]
    fn child_holds_the_lock() {
        use std::time::{Duration, Instant};

        let Ok(ws) = std::env::var(CHILD_ENV) else { return };
        let ws = PathBuf::from(ws);
        let Some(_lock) = try_acquire(&ws, "cross-process") else {
            fs::write(ws.join("failed"), b"").unwrap();
            return;
        };
        fs::write(ws.join("held"), b"").unwrap();
        // Hold until the parent has finished measuring, with a ceiling so a
        // parent that dies cannot leave this process running forever.
        let deadline = Instant::now() + Duration::from_secs(30);
        while !ws.join("release").exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Set by the parent to put the child half into child mode.
    const CHILD_ENV: &str = "DUCKLE_RUNLOCK_CHILD_WORKSPACE";

    #[test]
    fn keys_that_look_like_paths_cannot_escape_the_lock_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();
        let lock = try_acquire(ws, "../../etc/passwd").expect("sanitised, not rejected");
        drop(lock);
        let locks = ws.join(".duckle").join("locks");
        let stray = fs::read_dir(&locks).unwrap().filter_map(|e| e.ok()).count();
        assert_eq!(stray, 1, "the lock landed outside {}", locks.display());
    }

    /// "Someone else has it" and "I cannot lock here" must not look alike.
    ///
    /// Both collapsed to `None`, so the scheduler reported a pipeline as
    /// already running in another process when the truth was a workspace it
    /// could not write to - sending an operator to hunt for a process that was
    /// never there, while every tick went on skipping the run.
    #[test]
    fn a_held_lock_and_an_unlockable_workspace_are_told_apart() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();

        // Contention: a real second attempt while the first is held.
        let held = try_acquire(ws, "nightly-load").expect("first caller wins");
        match try_acquire_reason(ws, "nightly-load") {
            AcquireOutcome::HeldByOther => {}
            other => panic!("a held lock should read as HeldByOther, got {other:?}"),
        }
        drop(held);

        // ...and once released it is claimable again, so HeldByOther really is
        // about the holder and not about the path.
        assert!(
            matches!(try_acquire_reason(ws, "nightly-load"), AcquireOutcome::Claimed(_)),
            "the lock did not come back after the holder let go"
        );

        // Unusable: the lock directory's own path is occupied by a FILE, so it
        // cannot be created. No mocking - the OS reports the real error.
        let blocked = tempfile::tempdir().unwrap();
        std::fs::write(blocked.path().join(".duckle"), b"not a directory").unwrap();
        match try_acquire_reason(blocked.path(), "nightly-load") {
            AcquireOutcome::Unusable(e) => {
                assert!(!e.to_string().is_empty(), "the reason has to say something");
            }
            other => panic!("an unwritable workspace should read as Unusable, got {other:?}"),
        }

        // And the old API still answers the only question it ever asked.
        assert!(try_acquire(blocked.path(), "nightly-load").is_none());

        // A second, different unusable case, and the one that actually
        // exercises the platform error classification: the directories are all
        // creatable, but the lock FILE's own path is a directory, so the OPEN
        // fails with something that is not contention. Without this the
        // classifier could call every open failure "held" and nothing here
        // would notice.
        let odd = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(lock_path(odd.path(), "nightly-load")).unwrap();
        match try_acquire_reason(odd.path(), "nightly-load") {
            AcquireOutcome::Unusable(_) => {}
            other => panic!("a lock path that is a directory should be Unusable, got {other:?}"),
        }
    }

    /// A store that cannot be locked fails now, not in five seconds.
    #[test]
    fn lock_store_does_not_wait_out_a_workspace_it_can_never_lock() {
        let blocked = tempfile::tempdir().unwrap();
        std::fs::write(blocked.path().join(".duckle"), b"not a directory").unwrap();

        let started = std::time::Instant::now();
        let err = lock_store(blocked.path(), "schedules").expect_err("it claimed a lock it cannot take");
        // The retry ceiling is 5s; an unusable path must not spend it, and must
        // not report the timeout message, which blames a writer that is not there.
        assert!(started.elapsed() < std::time::Duration::from_secs(2), "it waited out the retry loop");
        assert!(err.contains("cannot lock"), "wrong reason: {err}");
    }

    /// Two different pipelines must never share one lock.
    ///
    /// Sanitising a name to a filename is many-to-one, and the collision was
    /// not exotic: `sales.daily` and `sales_daily` both became `sales_daily`,
    /// and any two non-ASCII names of equal length became the same run of
    /// underscores. The result was a scheduled run silently skipped, logged as
    /// a clash with a different pipeline that was merely running at the time.
    #[test]
    fn two_pipelines_whose_names_sanitise_alike_do_not_share_a_lock() {
        let tmp = tempfile::tempdir().unwrap();
        let ws = tmp.path();

        // The property, stated on the names themselves.
        assert_ne!(safe_name("sales.daily"), safe_name("sales_daily"));
        assert_ne!(safe_name("ventes/quotidien"), safe_name("ventes.quotidien"));
        // Non-ASCII collapsed hardest: every name of a given length was one
        // key. These share not a single ASCII character.
        assert_ne!(safe_name("日次"), safe_name("週次"));
        // And the same name is still the same lock, which is the whole point.
        assert_eq!(safe_name("sales.daily"), safe_name("sales.daily"));

        // The behaviour that follows from it: holding one leaves the other
        // free, so a schedule is not skipped because of an unrelated run.
        let held = try_acquire(ws, "sales.daily").expect("first pipeline could not lock");
        let other = try_acquire(ws, "sales_daily")
            .expect("an unrelated pipeline was blocked by a name collision");
        // ...while a genuine second attempt at the same pipeline is refused.
        assert!(try_acquire(ws, "sales.daily").is_none(), "the same pipeline locked twice");
        drop(held);
        drop(other);
    }

    /// The lock filename cannot drift between builds sharing a workspace.
    #[test]
    fn the_digest_is_fixed_rather_than_whatever_the_toolchain_hashes_to() {
        // Pinned values. DefaultHasher would satisfy the uniqueness test above
        // and still break the cross-process guarantee, because its output is
        // not stable across Rust releases - a desktop app and a runner built
        // on different toolchains would lock different files and neither would
        // be locking anything. These are FNV-1a, which is fixed forever.
        assert_eq!(digest(""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(digest("a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(digest("foobar"), 0x85944171f73967e8);
    }
}
