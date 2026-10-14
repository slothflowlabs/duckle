//! Self-extracting single-file artifact format.
//!
//! A built "Build Pipeline" artifact is ONE executable file laid out as:
//!
//!   [ stub runner bytes ][ zip payload bytes ][ 16-byte trailer ]
//!
//! The stub is a clean, trailer-free copy of the headless `duckle-runner`.
//! The zip payload carries everything the pipeline needs to run offline
//! (resolved pipeline JSON, contexts, routines, duckdb, extensions, secret
//! files, manifest). The 16-byte trailer at EOF lets the runner detect, at
//! startup, that it is running AS an artifact (vs the plain CLI) and where
//! the payload begins.
//!
//! Trailer (exactly 16 bytes at EOF):
//!   bytes 0..8  = MAGIC = b"DUCKLE01"
//!   bytes 8..16 = u64 little-endian = length of the ZIP PAYLOAD ONLY
//!                 (excludes the stub, excludes the 16 trailer bytes).
//!
//! Read algorithm (used by detect / has_trailer):
//!   N = file len
//!   if N < 16            -> NO trailer
//!   tail = bytes[N-16..N]
//!   if tail[0..8] != MAGIC -> NO trailer
//!   L = u64::from_le_bytes(tail[8..16])
//!   if 16 + L > N        -> NO trailer (malformed / false magic hit)
//!   payload = bytes[N-16-L .. N-16]
//!   stub    = bytes[0 .. N-16-L]
//!
//! This relies on PE / ELF / Mach-O executables tolerating arbitrary
//! TRAILING bytes appended after their real end: the OS loader reads only
//! the headers and section table and ignores the rest. It works for
//! unsigned binaries only - appending bytes invalidates a macOS codesign
//! signature or a Windows Authenticode signature. Artifacts are produced
//! unsigned; this is documented in the manifest as well.

use sha2::{Digest, Sha256};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipArchive, ZipWriter};

/// 8-byte constant that marks a Duckle self-extracting artifact.
pub const MAGIC: [u8; 8] = *b"DUCKLE01";

/// Total trailer size: 8-byte magic + 8-byte little-endian payload length.
const TRAILER_LEN: u64 = 16;

/// Pack a staging directory into a DEFLATE zip payload (files only; the
/// extractor recreates the directory tree). Relative entry paths use
/// forward slashes so a Windows-built artifact unpacks identically on unix.
pub fn pack(staging_dir: &Path) -> Result<Vec<u8>, String> {
    let mut files: Vec<PathBuf> = Vec::new();
    collect_files(staging_dir, &mut files)?;
    files.sort();

    let cursor = std::io::Cursor::new(Vec::new());
    let mut zip = ZipWriter::new(cursor);
    let opts = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);

    for path in &files {
        let rel = path
            .strip_prefix(staging_dir)
            .map_err(|e| format!("strip prefix {}: {}", path.display(), e))?;
        let rel = rel.to_string_lossy().replace('\\', "/");
        zip.start_file(rel, opts)
            .map_err(|e| format!("zip start_file: {}", e))?;
        let bytes = std::fs::read(path).map_err(|e| format!("read {}: {}", path.display(), e))?;
        zip.write_all(&bytes)
            .map_err(|e| format!("zip write: {}", e))?;
    }

    let cursor = zip.finish().map_err(|e| format!("zip finish: {}", e))?;
    Ok(cursor.into_inner())
}

/// Recursively collect every regular file under `dir`.
fn collect_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<(), String> {
    let entries =
        std::fs::read_dir(dir).map_err(|e| format!("read_dir {}: {}", dir.display(), e))?;
    for entry in entries {
        let entry = entry.map_err(|e| format!("read_dir entry: {}", e))?;
        let path = entry.path();
        let ft = entry
            .file_type()
            .map_err(|e| format!("file_type {}: {}", path.display(), e))?;
        if ft.is_dir() {
            collect_files(&path, out)?;
        } else if ft.is_file() {
            out.push(path);
        }
    }
    Ok(())
}

/// Write the final single-file artifact: stub bytes, then payload, then the
/// 16-byte trailer. On unix the result is marked executable (0o755).
pub fn write_artifact(stub: &[u8], payload: &[u8], out: &Path) -> Result<(), String> {
    let mut buf = Vec::with_capacity(stub.len() + payload.len() + TRAILER_LEN as usize);
    buf.extend_from_slice(stub);
    buf.extend_from_slice(payload);
    buf.extend_from_slice(&MAGIC);
    buf.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    std::fs::write(out, &buf).map_err(|e| format!("write {}: {}", out.display(), e))?;
    set_exec_755(out)?;
    Ok(())
}

/// Inspect `exe`'s tail for a valid trailer and, if present, return the
/// embedded zip payload bytes. Returns Ok(None) for a plain (trailer-free)
/// executable. Opens the file with std::fs::File (NOT mmap) so a running
/// Windows artifact, opened deny-write but still readable, can read itself.
pub fn detect(exe: &Path) -> Result<Option<Vec<u8>>, String> {
    let mut f = std::fs::File::open(exe).map_err(|e| format!("open {}: {}", exe.display(), e))?;
    let n = f
        .metadata()
        .map_err(|e| format!("stat {}: {}", exe.display(), e))?
        .len();
    if n < TRAILER_LEN {
        return Ok(None);
    }
    f.seek(SeekFrom::Start(n - TRAILER_LEN))
        .map_err(|e| format!("seek {}: {}", exe.display(), e))?;
    let mut tail = [0u8; TRAILER_LEN as usize];
    f.read_exact(&mut tail)
        .map_err(|e| format!("read trailer {}: {}", exe.display(), e))?;
    if tail[0..8] != MAGIC {
        return Ok(None);
    }
    let mut len_bytes = [0u8; 8];
    len_bytes.copy_from_slice(&tail[8..16]);
    let payload_len = u64::from_le_bytes(len_bytes);
    if TRAILER_LEN + payload_len > n {
        // Malformed or a false magic hit inside an ordinary binary.
        return Ok(None);
    }
    let payload_start = n - TRAILER_LEN - payload_len;
    f.seek(SeekFrom::Start(payload_start))
        .map_err(|e| format!("seek payload {}: {}", exe.display(), e))?;
    let mut payload = vec![0u8; payload_len as usize];
    f.read_exact(&mut payload)
        .map_err(|e| format!("read payload {}: {}", exe.display(), e))?;
    Ok(Some(payload))
}

/// Whether `exe` carries a valid artifact trailer (cheap tail-only check).
pub fn has_trailer(exe: &Path) -> Result<bool, String> {
    let mut f = std::fs::File::open(exe).map_err(|e| format!("open {}: {}", exe.display(), e))?;
    let n = f
        .metadata()
        .map_err(|e| format!("stat {}: {}", exe.display(), e))?
        .len();
    if n < TRAILER_LEN {
        return Ok(false);
    }
    f.seek(SeekFrom::Start(n - TRAILER_LEN))
        .map_err(|e| format!("seek {}: {}", exe.display(), e))?;
    let mut tail = [0u8; TRAILER_LEN as usize];
    f.read_exact(&mut tail)
        .map_err(|e| format!("read trailer {}: {}", exe.display(), e))?;
    if tail[0..8] != MAGIC {
        return Ok(false);
    }
    let mut len_bytes = [0u8; 8];
    len_bytes.copy_from_slice(&tail[8..16]);
    let payload_len = u64::from_le_bytes(len_bytes);
    Ok(TRAILER_LEN + payload_len <= n)
}

/// Extract the payload to a per-artifact cache dir keyed by its SHA-256, so
/// repeat runs of the same artifact reuse the extraction and concurrent
/// runs of the same (or different) artifacts do not collide.
///
/// Idempotency + concurrency: the cache dir is "ready" only when it contains
/// a `.duckle-ok` marker. A run that finds the marker returns immediately. A
/// run that does not unpacks into a unique temp dir, writes `.duckle-ok`
/// LAST, then atomically renames the temp dir into place. If the rename
/// loses a race (another process already populated the dir), the marker is
/// re-checked and the loser's temp dir is discarded.
///
/// The temp cache accrues one extracted copy per distinct artifact hash and
/// is not auto-pruned (a future LRU could reclaim it); the same artifact run
/// repeatedly reuses its extraction instantly.
/// Where extracted artifacts live: a directory belonging to this user.
///
/// GHSA-58g9-237h-ch2g. The per-artifact directory name is `sha256(payload)`,
/// and the payload's boundaries are public - a 16-byte trailer anyone holding
/// the file can read - so every user on the host computes the same name. In a
/// shared `/tmp` that is a name an attacker can claim before the victim's first
/// run, and `/tmp`'s sticky bit does not stop creating an entry, only removing
/// someone else's.
///
/// So the cache is not in the shared temp dir. `XDG_CACHE_HOME` or `~/.cache`
/// on unix, `LOCALAPPDATA` on Windows; the process temp dir only when there is
/// no home to use, where [`is_private_to_us`] is what still has to hold.
pub(crate) fn cache_base() -> PathBuf {
    let home = if cfg!(windows) {
        std::env::var_os("LOCALAPPDATA").map(PathBuf::from)
    } else {
        std::env::var_os("XDG_CACHE_HOME")
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".cache")))
    };
    match home {
        Some(h) => h.join("duckle").join("artifacts"),
        None => std::env::temp_dir().join("duckle").join("artifacts"),
    }
}

/// Is this directory one only we could have written?
///
/// The `.duckle-ok` marker used to be the whole trust decision, and anyone who
/// could create the directory could create the marker - after which `bin/duckdb`
/// is whatever they left there, and the engine spawns it once per SQL stage as
/// the victim.
///
/// On unix that means owned by the effective uid and not writable by group or
/// other. On Windows there is no uid to compare: `LOCALAPPDATA` is per-user and
/// carries its own ACL, which is what stands in for this.
pub(crate) fn is_private_to_us(dir: &Path) -> bool {
    let Ok(meta) = std::fs::metadata(dir) else {
        return false;
    };
    if !meta.is_dir() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        // SAFETY: geteuid cannot fail and touches no memory.
        let me = unsafe { libc::geteuid() };
        if meta.uid() != me {
            return false;
        }
        if meta.mode() & 0o022 != 0 {
            return false;
        }
    }
    true
}

pub fn extract_to_cache(payload: &[u8]) -> Result<PathBuf, String> {
    let key = hex(&Sha256::digest(payload));
    let base = cache_base();
    let root = base.join(format!("duckle-artifact-{key}"));
    let ok = root.join(".duckle-ok");
    if ok.exists() {
        // Reuse only what we could have written ourselves. A cache directory
        // somebody else owns, or that anyone can write to, is refused rather
        // than repaired: re-extracting into it would leave whatever they planted
        // beside what we unpack.
        if !is_private_to_us(&root) {
            return Err(format!(
                "refusing to reuse the artifact cache at {}: it is not owned by this user, or \
                 it is writable by others. Remove it and run this again. Anything under that \
                 path could have been put there by someone else, and this runner would then \
                 execute it.",
                root.display()
            ));
        }
        return Ok(root);
    }

    // The base has to exist, and has to be ours, before anything is unpacked
    // beneath it. Made 0700 on unix so the staging directory and the finished
    // one are both out of everybody else's reach.
    std::fs::create_dir_all(&base).map_err(|e| format!("mkdir {}: {}", base.display(), e))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&base, std::fs::Permissions::from_mode(0o700));
    }
    if !is_private_to_us(&base) {
        return Err(format!(
            "refusing to use the artifact cache at {}: it is not owned by this user, or it is \
             writable by others. Remove it and run this again.",
            base.display()
        ));
    }

    let mut rand = [0u8; 8];
    getrandom::fill(&mut rand).map_err(|e| format!("rand: {}", e))?;
    // Staged inside the same base, not in the shared temp dir: the rename below
    // has to stay on one filesystem, and the half-unpacked copy should be no
    // more reachable than the finished one.
    let tmp = base.join(format!(
        "duckle-artifact-{key}-tmp-{}-{}",
        std::process::id(),
        hex(&rand)
    ));
    if tmp.exists() {
        let _ = std::fs::remove_dir_all(&tmp);
    }
    std::fs::create_dir_all(&tmp).map_err(|e| format!("mkdir {}: {}", tmp.display(), e))?;

    if let Err(e) = unzip_into(payload, &tmp) {
        let _ = std::fs::remove_dir_all(&tmp);
        return Err(e);
    }
    // Marker written LAST so a half-written dir is never seen as ready.
    std::fs::write(tmp.join(".duckle-ok"), b"ok")
        .map_err(|e| format!("write ok marker: {}", e))?;

    match std::fs::rename(&tmp, &root) {
        Ok(()) => Ok(root),
        Err(e) => {
            let _ = std::fs::remove_dir_all(&tmp);
            // Another process may have won the race and populated root - but
            // "somebody got there first" and "somebody planted it" look
            // identical from here, so losing the race is not on its own a reason
            // to trust what is now in the way.
            if ok.exists() && is_private_to_us(&root) {
                Ok(root)
            } else if ok.exists() {
                Err(format!(
                    "refusing to reuse the artifact cache at {}: it is not owned by this user, \
                     or it is writable by others. Remove it and run this again.",
                    root.display()
                ))
            } else {
                Err(format!("rename {} -> {}: {}", tmp.display(), root.display(), e))
            }
        }
    }
}

/// Unzip a payload into `dest`, sanitizing entry names (reject absolute
/// paths and any `..` component) and recreating parent dirs. On unix the
/// bundled `bin/duckdb` is made executable.
fn unzip_into(payload: &[u8], dest: &Path) -> Result<(), String> {
    let cursor = std::io::Cursor::new(payload);
    let mut archive = ZipArchive::new(cursor).map_err(|e| format!("open zip: {}", e))?;
    for i in 0..archive.len() {
        let mut entry = archive
            .by_index(i)
            .map_err(|e| format!("zip entry {}: {}", i, e))?;
        let name = entry
            .name()
            .map_err(|e| format!("zip entry {}: {}", i, e))?
            .into_owned();
        let rel = sanitize_entry(&name)?;
        let out_path = dest.join(&rel);
        if let Some(parent) = out_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("mkdir {}: {}", parent.display(), e))?;
        }
        let mut bytes = Vec::with_capacity(entry.size() as usize);
        entry
            .read_to_end(&mut bytes)
            .map_err(|e| format!("read entry {}: {}", name, e))?;
        std::fs::write(&out_path, &bytes)
            .map_err(|e| format!("write {}: {}", out_path.display(), e))?;
    }
    // The bundled duckdb must be runnable after extraction on unix.
    set_exec_755(&dest.join("bin").join("duckdb"))?;
    Ok(())
}

/// Validate a zip entry name: reject absolute paths and `..` traversal.
/// Returns a relative PathBuf with platform separators.
fn sanitize_entry(name: &str) -> Result<PathBuf, String> {
    let norm = name.replace('\\', "/");
    if norm.starts_with('/') || norm.contains(':') {
        return Err(format!("unsafe zip entry (absolute path): {}", name));
    }
    let mut out = PathBuf::new();
    for seg in norm.split('/') {
        if seg.is_empty() || seg == "." {
            continue;
        }
        if seg == ".." {
            return Err(format!("unsafe zip entry (parent traversal): {}", name));
        }
        out.push(seg);
    }
    Ok(out)
}

/// Lowercase hex of a byte slice.
fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

/// Set the unix exec bit (0o755) on `path` if it exists. No-op on windows
/// and when the file is absent.
#[cfg(unix)]
fn set_exec_755(path: &Path) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    if !path.exists() {
        return Ok(());
    }
    let mut perms = std::fs::metadata(path)
        .map_err(|e| format!("stat {}: {}", path.display(), e))?
        .permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(path, perms)
        .map_err(|e| format!("chmod {}: {}", path.display(), e))
}

#[cfg(not(unix))]
fn set_exec_755(_path: &Path) -> Result<(), String> {
    Ok(())
}

#[cfg(test)]
mod cache_trust {
    use super::*;

    /// GHSA-58g9-237h-ch2g: the cache path is a pure function of the payload,
    /// and the payload's boundaries are public, so anyone holding the artifact
    /// computes the same directory name. On a shared `/tmp` that is a place an
    /// attacker can reach before the victim's first run.
    ///
    /// It must not be a shared directory at all.
    #[test]
    fn the_artifact_cache_is_private_to_this_user() {
        let base = cache_base();
        let shared = std::env::temp_dir();
        // On Windows the process temp dir is already per-user, so equality there
        // is not the failure it is on unix.
        if cfg!(unix) {
            assert_ne!(
                base, shared,
                "the artifact cache must not sit directly in the shared temp dir",
            );
        }
        assert!(
            base.ends_with("artifacts"),
            "expected a duckle-owned artifacts dir, got {}",
            base.display(),
        );
    }

    /// The marker file was the whole trust decision: `.duckle-ok` present meant
    /// "this was extracted, use it". Anyone who could create the directory could
    /// create the marker, and then `bin/duckdb` is whatever they put there - and
    /// the engine spawns it once per SQL stage, as the victim.
    ///
    /// Unix only: the check is uid and mode, and Windows has neither. A per-user
    /// LOCALAPPDATA with its own ACL is what carries it there.
    #[cfg(unix)]
    #[test]
    fn a_cache_directory_anyone_could_have_written_is_not_trusted() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().unwrap();
        let planted = tmp.path().join("duckle-artifact-deadbeef");
        std::fs::create_dir_all(planted.join("bin")).unwrap();
        std::fs::write(planted.join("bin").join("duckdb"), b"#!/bin/sh\necho pwned\n").unwrap();
        std::fs::write(planted.join(".duckle-ok"), b"ok").unwrap();

        // Group- and world-writable: what a directory planted in a shared temp
        // looks like, and what our own 0700 extraction never looks like.
        std::fs::set_permissions(&planted, std::fs::Permissions::from_mode(0o777)).unwrap();
        assert!(
            !is_private_to_us(&planted),
            "a world-writable cache directory was accepted as our own",
        );

        // The same directory, locked down, is ours to reuse.
        std::fs::set_permissions(&planted, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(
            is_private_to_us(&planted),
            "a 0700 directory we own is exactly what extraction leaves behind",
        );
    }

    /// A missing directory is not a trusted one. Guards the ordering: the caller
    /// asks this before deciding to skip extraction.
    #[test]
    fn a_directory_that_is_not_there_is_not_trusted() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(!is_private_to_us(&tmp.path().join("nothing-here")));
    }
}
