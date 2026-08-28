//! Engine installation manager.
//!
//! Duckle ships a tiny shell and downloads its execution engines on
//! first launch into the app-data directory, rather than statically
//! bundling them. DuckDB and SlothDB install through one shared path:
//! fetch the platform's release zip from GitHub, extract the binary,
//! mark it executable, and verify it runs.

use serde::Serialize;
use std::io::Read;
use std::path::{Path, PathBuf};

pub const DUCKDB_VERSION: &str = "1.5.4";
pub const SLOTHDB_VERSION: &str = "0.2.7";
/// Pinned llama.cpp build. Bump periodically; the GGUF wire format
/// is stable so newer server binaries keep working with older models.
/// Note: assets at older builds use a different naming (avx/avx2/cuda
/// flavors) - keep this on a recent build that ships the `*-cpu-*`
/// universal variant.
pub const LLAMACPP_BUILD: &str = "b9305";
/// A GGUF chat model the assistant can be installed with.
///
/// The catalogue is curated rather than a live Hugging Face search: every
/// entry is a repo + filename that has been checked to resolve, so the picker
/// cannot offer something that 404s halfway through a multi-gigabyte download.
/// All are Q4_K_M quantisations, which is the size/quality knee for local use.
#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct LlamaModel {
    /// Stable id stored in settings. Never reuse one for a different file.
    pub id: &'static str,
    pub label: &'static str,
    pub repo: &'static str,
    pub file: &'static str,
    /// Real download size, from the Hugging Face file listing.
    pub size_mb: u32,
    /// What this choice costs and buys, in the user's terms.
    pub note: &'static str,
}

/// Models offered at install time, smallest first.
///
/// Bigger is not automatically better here: the assistant runs on the user's
/// own machine, so a 14B model on a laptop with no GPU offload is slower than
/// it is useful. The notes say so rather than leaving it to be discovered
/// after an 8.5 GB download.
pub const LLAMA_MODELS: &[LlamaModel] = &[
    LlamaModel {
        id: "qwen2.5-coder-0.5b",
        label: "Qwen2.5 Coder 0.5B",
        repo: "Qwen/Qwen2.5-Coder-0.5B-Instruct-GGUF",
        file: "qwen2.5-coder-0.5b-instruct-q4_k_m.gguf",
        size_mb: 469,
        note: "Smallest option. For low-RAM or older machines; expect rough answers.",
    },
    LlamaModel {
        id: "llama-3.2-1b",
        label: "Llama 3.2 1B Instruct",
        repo: "bartowski/Llama-3.2-1B-Instruct-GGUF",
        file: "Llama-3.2-1B-Instruct-Q4_K_M.gguf",
        size_mb: 770,
        note: "Very fast and light. Better at plain English than at pipeline JSON.",
    },
    LlamaModel {
        id: "qwen2.5-coder-1.5b",
        label: "Qwen2.5 Coder 1.5B",
        repo: "Qwen/Qwen2.5-Coder-1.5B-Instruct-GGUF",
        file: "qwen2.5-coder-1.5b-instruct-q4_k_m.gguf",
        size_mb: 1066,
        note: "Default. Runs on any modern laptop CPU. Best choice if you are unsure.",
    },
    LlamaModel {
        id: "llama-3.2-3b",
        label: "Llama 3.2 3B Instruct",
        repo: "bartowski/Llama-3.2-3B-Instruct-GGUF",
        file: "Llama-3.2-3B-Instruct-Q4_K_M.gguf",
        size_mb: 1926,
        note: "Stronger at plain English, weaker at code than the Qwen models.",
    },
    LlamaModel {
        id: "qwen2.5-coder-3b",
        label: "Qwen2.5 Coder 3B",
        repo: "Qwen/Qwen2.5-Coder-3B-Instruct-GGUF",
        file: "qwen2.5-coder-3b-instruct-q4_k_m.gguf",
        size_mb: 2007,
        note: "Noticeably better pipeline generation. Comfortable on 16 GB of RAM.",
    },
    LlamaModel {
        id: "phi-3.5-mini",
        label: "Phi-3.5 Mini",
        repo: "bartowski/Phi-3.5-mini-instruct-GGUF",
        file: "Phi-3.5-mini-instruct-Q4_K_M.gguf",
        size_mb: 2282,
        note: "Strong reasoning for its size. Good all-rounder on a mid-range laptop.",
    },
    LlamaModel {
        id: "qwen3-4b",
        label: "Qwen3 4B Instruct",
        repo: "unsloth/Qwen3-4B-Instruct-2507-GGUF",
        file: "Qwen3-4B-Instruct-2507-Q4_K_M.gguf",
        size_mb: 2382,
        note: "Newer generation. Best quality per gigabyte in the mid sizes.",
    },
    LlamaModel {
        id: "mistral-7b",
        label: "Mistral 7B Instruct v0.3",
        repo: "bartowski/Mistral-7B-Instruct-v0.3-GGUF",
        file: "Mistral-7B-Instruct-v0.3-Q4_K_M.gguf",
        size_mb: 4170,
        note: "Well-rounded general model. Wants 16 GB of RAM or GPU offload.",
    },
    LlamaModel {
        id: "qwen2.5-coder-7b",
        label: "Qwen2.5 Coder 7B",
        repo: "Qwen/Qwen2.5-Coder-7B-Instruct-GGUF",
        file: "qwen2.5-coder-7b-instruct-q4_k_m.gguf",
        size_mb: 4466,
        note: "The best code model here that still fits a 8 GB GPU. Slow on CPU alone.",
    },
    LlamaModel {
        id: "qwen2.5-7b",
        label: "Qwen2.5 7B Instruct",
        repo: "bartowski/Qwen2.5-7B-Instruct-GGUF",
        file: "Qwen2.5-7B-Instruct-Q4_K_M.gguf",
        size_mb: 4466,
        note: "General-purpose sibling of the 7B coder. Better prose, weaker code.",
    },
    LlamaModel {
        id: "qwen3-8b",
        label: "Qwen3 8B",
        repo: "unsloth/Qwen3-8B-GGUF",
        file: "Qwen3-8B-Q4_K_M.gguf",
        size_mb: 4795,
        note: "Newer generation at 8B. Wants a GPU with 8 GB or more.",
    },
    LlamaModel {
        id: "gemma-2-9b",
        label: "Gemma 2 9B",
        repo: "bartowski/gemma-2-9b-it-GGUF",
        file: "gemma-2-9b-it-Q4_K_M.gguf",
        size_mb: 5494,
        note: "Google's 9B. Strong general answers; needs real GPU memory.",
    },
    LlamaModel {
        id: "qwen2.5-coder-14b",
        label: "Qwen2.5 Coder 14B",
        repo: "Qwen/Qwen2.5-Coder-14B-Instruct-GGUF",
        file: "qwen2.5-coder-14b-instruct-q4_k_m.gguf",
        size_mb: 8572,
        note: "Only worth it with a GPU that has 10 GB or more of memory.",
    },
    LlamaModel {
        id: "deepseek-coder-v2-lite",
        label: "DeepSeek Coder V2 Lite",
        repo: "bartowski/DeepSeek-Coder-V2-Lite-Instruct-GGUF",
        file: "DeepSeek-Coder-V2-Lite-Instruct-Q4_K_M.gguf",
        size_mb: 9884,
        note: "Largest option. A mixture-of-experts coder; needs 12 GB of GPU memory.",
    },
];

/// The model used when the user has not chosen one.
pub const DEFAULT_LLAMA_MODEL_ID: &str = "qwen2.5-coder-1.5b";

/// Look a model up by id, falling back to the default.
///
/// An unknown id resolves to the default rather than failing: a settings file
/// written by a newer build, or a catalogue entry we later retire, should not
/// leave the assistant uninstallable.
pub fn llama_model(id: Option<&str>) -> &'static LlamaModel {
    let want = id.unwrap_or(DEFAULT_LLAMA_MODEL_ID);
    LLAMA_MODELS
        .iter()
        .find(|m| m.id == want)
        .or_else(|| LLAMA_MODELS.iter().find(|m| m.id == DEFAULT_LLAMA_MODEL_ID))
        .unwrap_or(&LLAMA_MODELS[0])
}

/// Static description of an installable engine.
struct EngineSpec {
    id: &'static str,
    name: &'static str,
    description: &'static str,
    required: bool,
    repo: &'static str,
    version: &'static str,
    /// Binary base name (without the .exe suffix).
    binary: &'static str,
}

const DUCKDB: EngineSpec = EngineSpec {
    id: "duckdb",
    name: "DuckDB",
    description: "Default engine - local analytics, file formats, SQL.",
    required: true,
    repo: "duckdb/duckdb",
    version: DUCKDB_VERSION,
    binary: "duckdb",
};

const SLOTHDB: EngineSpec = EngineSpec {
    id: "slothdb",
    name: "SlothDB",
    description: "Optional embedded engine. Downloads from the SlothDB releases.",
    required: false,
    repo: "SouravRoy-ETL/slothdb",
    version: SLOTHDB_VERSION,
    binary: "slothdb",
};

/// llama.cpp HTTP server + a small Qwen GGUF model. Treated as an
/// optional "engine" for UX consistency with the setup screen but
/// powers the Duckie AI Assistant chat panel rather than the SQL
/// execution path.
const LLAMACPP: EngineSpec = EngineSpec {
    id: "llamacpp",
    name: "Duckie AI Assistant",
    description: "Local chat assistant via llama.cpp + Qwen 1.5B. Downloads ~1.1 GB; runs entirely offline once installed.",
    required: false,
    // Repo moved from ggerganov to ggml-org in mid-2025; use the new
    // org path directly to skip the 301 redirect.
    repo: "ggml-org/llama.cpp",
    version: LLAMACPP_BUILD,
    binary: "llama-server",
};

const ENGINES: [&EngineSpec; 3] = [&DUCKDB, &SLOTHDB, &LLAMACPP];

fn spec(id: &str) -> Option<&'static EngineSpec> {
    ENGINES.iter().copied().find(|e| e.id == id)
}

fn binary_file_name(s: &EngineSpec) -> String {
    if cfg!(windows) {
        format!("{}.exe", s.binary)
    } else {
        s.binary.to_string()
    }
}

/// Marks an artifact whose SHA-256 has not been recorded yet.
///
/// An unpinned download proceeds with a warning rather than failing, because
/// refusing outright would break every install until this table is filled in.
/// Replace one of the constants below with a real digest and verification becomes
/// mandatory for that artifact immediately: there is no second switch to remember,
/// and no way to pin a digest without also enforcing it.
pub(crate) const UNPINNED: &str = "UNPINNED";

/// SHA-256 of every artifact Duckle downloads and then EXECUTES.
///
/// Until these are filled in, the only thing standing between a user and a
/// substituted binary is TLS to the host serving it. That is not nothing, but it
/// trusts the host, anyone who can obtain a certificate for it, and every redirect
/// in between - and two of these fetch a MUTABLE reference (`releases/latest` and
/// `resolve/main`), so the bytes behind the URL can change without the URL doing so.
///
/// To fill one in: download the exact asset the constant names, run
/// `sha256sum <file>` (or `Get-FileHash -Algorithm SHA256`), and paste the hex.
/// Pin the upstream reference to a tag or revision at the same time, because a
/// digest against a moving target only turns a silent substitution into a failed
/// install.
pub(crate) const DUCKDB_SHA256: &str = UNPINNED;
pub(crate) const UV_SHA256: &str = UNPINNED;
pub(crate) const FUSION_SHA256: &str = UNPINNED;

/// Per-engine digests, keyed by spec id and version so a version bump cannot keep
/// silently matching an old pin.
pub(crate) fn expected_engine_sha256(id: &str, version: &str) -> &'static str {
    match (id, version) {
        // ("llamacpp", "b1234") => "…",
        // ("slothdb", "1.5.4") => "…",
        _ => UNPINNED,
    }
}

/// Per-model digest, keyed by the Hugging Face repo and file.
pub(crate) fn expected_model_sha256(repo: &str, file: &str) -> &'static str {
    match (repo, file) {
        // ("Qwen/…", "…gguf") => "…",
        _ => UNPINNED,
    }
}

pub(crate) fn hex_sha256(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(bytes);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

/// Compare a computed digest against the pinned one.
///
/// Fails closed: a mismatch is an error, never a warning, because the artifact is
/// about to be executed. The caller must delete any partially written file before
/// propagating, or the next run finds it on disk and skips the download entirely.
pub(crate) fn verify_download(label: &str, actual_hex: &str, expected: &str) -> Result<(), String> {
    if expected == UNPINNED {
        eprintln!(
            "duckle: {label} is not pinned to a checksum, so only TLS vouches for it. See UNPINNED in engine_manager.rs."
        );
        return Ok(());
    }
    if !actual_hex.eq_ignore_ascii_case(expected) {
        return Err(format!(
            "{label} failed checksum verification and was discarded. Expected {expected}, got {actual_hex}. Either the download was tampered with, or the pinned digest is stale after a version bump."
        ));
    }
    Ok(())
}

fn engine_dir(app_data: &Path, s: &EngineSpec) -> PathBuf {
    app_data.join("engines").join(s.id)
}

fn binary_path(app_data: &Path, s: &EngineSpec) -> PathBuf {
    engine_dir(app_data, s).join(binary_file_name(s))
}

/// A small file recording which version of an engine's binary is installed,
/// written on install. Without it, status() can only check that *a* binary
/// exists, so a version bump (e.g. DuckDB 1.5.3 -> 1.5.4) would keep the stale
/// binary and never re-download. Reading the stamp lets status() detect the
/// mismatch and re-run the one-click install over the old binary.
fn version_stamp_path(app_data: &Path, s: &EngineSpec) -> PathBuf {
    engine_dir(app_data, s).join(".installed-version")
}

/// The version recorded on disk for an engine, if any (None for a stamp-less
/// pre-existing install or a missing engine).
fn installed_version(app_data: &Path, s: &EngineSpec) -> Option<String> {
    std::fs::read_to_string(version_stamp_path(app_data, s))
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// Public helper kept for the engine() resolver in lib.rs.
pub fn duckdb_path(app_data: &Path) -> PathBuf {
    binary_path(app_data, &DUCKDB)
}

/// Path the AI assistant server binary lands at.
pub fn llamacpp_path(app_data: &Path) -> PathBuf {
    binary_path(app_data, &LLAMACPP)
}

/// Path the GGUF model file lands at (sibling of the binary).
///
/// Resolved by looking for whichever `.gguf` is actually in the engine
/// directory rather than by name, so the chat server starts against whatever
/// the user chose at install time. Falls back to the default model's filename
/// when nothing is installed yet, which is the path the installer writes to.
pub fn llama_model_path(app_data: &Path) -> PathBuf {
    let dir = engine_dir(app_data, &LLAMACPP);
    if let Ok(entries) = std::fs::read_dir(&dir) {
        let mut found: Vec<PathBuf> = entries
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().and_then(|x| x.to_str()) == Some("gguf"))
            .collect();
        // Deterministic when a previous model was left behind: the newest wins,
        // and ties break by name so the choice never flips between launches.
        found.sort();
        if let Some(newest) = found
            .iter()
            .max_by_key(|p| std::fs::metadata(p).and_then(|m| m.modified()).ok())
        {
            return newest.clone();
        }
    }
    dir.join(llama_model(None).file)
}

/// Release asset name for this OS/arch, or None if unsupported.
fn asset_for(s: &EngineSpec) -> Option<String> {
    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    match s.id {
        "duckdb" => Some(
            match (os, arch) {
                ("windows", "x86_64") => "duckdb_cli-windows-amd64.zip",
                ("windows", "aarch64") => "duckdb_cli-windows-arm64.zip",
                ("linux", "x86_64") => "duckdb_cli-linux-amd64.zip",
                ("linux", "aarch64") => "duckdb_cli-linux-arm64.zip",
                ("macos", _) => "duckdb_cli-osx-universal.zip",
                _ => return None,
            }
            .to_string(),
        ),
        // SlothDB ships raw, single-file binaries per its releases -
        // not zips. Names per https://github.com/SouravRoy-ETL/slothdb.
        "slothdb" => Some(
            match (os, arch) {
                ("windows", _) => "slothdb.exe",
                ("linux", "x86_64") => "slothdb-linux-x64",
                ("macos", _) => "slothdb-macos",
                _ => return None,
            }
            .to_string(),
        ),
        // llama.cpp ships pre-built binaries per OS/arch. We pick the
        // most-compatible variant (no GPU acceleration) so the model
        // runs on any CPU - the chat assistant only needs ~5 tok/s.
        // Windows ships as zip; Linux + macOS as tar.gz.
        "llamacpp" => Some(
            match (os, arch) {
                ("windows", "x86_64") => format!("llama-{}-bin-win-cpu-x64.zip", LLAMACPP_BUILD),
                ("windows", "aarch64") => format!("llama-{}-bin-win-cpu-arm64.zip", LLAMACPP_BUILD),
                ("linux", "x86_64") => format!("llama-{}-bin-ubuntu-x64.tar.gz", LLAMACPP_BUILD),
                ("linux", "aarch64") => format!("llama-{}-bin-ubuntu-arm64.tar.gz", LLAMACPP_BUILD),
                ("macos", "aarch64") => format!("llama-{}-bin-macos-arm64.tar.gz", LLAMACPP_BUILD),
                ("macos", _) => format!("llama-{}-bin-macos-x64.tar.gz", LLAMACPP_BUILD),
                _ => return None,
            },
        ),
        _ => None,
    }
}

/// DuckDB CLI release asset name for an arbitrary OS/arch (not necessarily the
/// host). Used to fetch a cross-target DuckDB when "Build Pipeline" targets a
/// different OS than the one Duckle runs on.
fn duckdb_asset(os: &str, arch: &str) -> Option<&'static str> {
    Some(match (os, arch) {
        ("windows", "x86_64") => "duckdb_cli-windows-amd64.zip",
        ("windows", "aarch64") => "duckdb_cli-windows-arm64.zip",
        ("linux", "x86_64") => "duckdb_cli-linux-amd64.zip",
        ("linux", "aarch64") => "duckdb_cli-linux-arm64.zip",
        ("macos", _) => "duckdb_cli-osx-universal.zip",
        _ => return None,
    })
}

/// Resolve a DuckDB CLI binary for a DIFFERENT target than the host, used to
/// assemble a cross-OS "Build Pipeline" artifact. Downloads the official
/// DuckDB release zip (same pinned DUCKDB_VERSION as the host engine) for the
/// requested os/arch and caches the extracted binary under
/// `engines/duckdb-cross/<os>-<arch>/duckdb(.exe)`. Returns the cached path.
///
/// The downloaded binary is for the TARGET OS, so the host cannot execute it;
/// it is only ever copied into the artifact payload. Its exec bit is set at
/// run time when the artifact self-extracts on the target (see selfextract).
pub fn ensure_cross_duckdb(app_data: &Path, os: &str, arch: &str) -> Result<PathBuf, String> {
    let asset = duckdb_asset(os, arch)
        .ok_or_else(|| format!("No DuckDB build for {}-{}", os, arch))?;
    let bin_name = if os == "windows" { "duckdb.exe" } else { "duckdb" };
    let dir = app_data
        .join("engines")
        .join("duckdb-cross")
        .join(format!("{}-{}", os, arch));
    let target = dir.join(bin_name);
    if target.exists() {
        return Ok(target);
    }
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;

    let url = format!(
        "https://github.com/{}/releases/download/v{}/{}",
        DUCKDB.repo, DUCKDB_VERSION, asset
    );
    let client = reqwest::blocking::Client::builder()
        .user_agent("duckle")
        .use_preconfigured_tls(duckle_duckdb_engine::tls::build_client_config())
        .build()
        .map_err(|e| e.to_string())?;
    let resp = client.get(&url).send().map_err(|e| e.to_string())?;
    if !resp.status().is_success() {
        return Err(format!(
            "Couldn't download DuckDB for {}-{} (HTTP {}). The release v{} may not exist yet.",
            os,
            arch,
            resp.status().as_u16(),
            DUCKDB_VERSION
        ));
    }
    let expected = resp.content_length();
    let bytes = resp.bytes().map_err(|e| e.to_string())?;
    // Reject a truncated transfer before baking the binary into a shipped
    // artifact: a short read here would otherwise produce a corrupt bundled
    // duckdb that only fails on the target. A DuckDB CLI zip is multi-MB, so a
    // tiny body also signals an error/redirect page slipped through.
    if let Some(expected) = expected {
        if (bytes.len() as u64) != expected {
            return Err(format!(
                "DuckDB download for {}-{} was truncated ({} of {} bytes)",
                os,
                arch,
                bytes.len(),
                expected
            ));
        }
    }
    if bytes.len() < 1_000_000 {
        return Err(format!(
            "DuckDB download for {}-{} is implausibly small ({} bytes); aborting",
            os,
            arch,
            bytes.len()
        ));
    }
    // Before the archive is opened, let alone the binary extracted and run.
    verify_download("the DuckDB CLI download", &hex_sha256(&bytes), DUCKDB_SHA256)?;

    let reader = std::io::Cursor::new(bytes);
    let mut archive = zip::ZipArchive::new(reader).map_err(|e| e.to_string())?;
    let mut extracted = false;
    // DuckDB CLI zips ship a single self-contained binary named duckdb(.exe).
    for i in 0..archive.len() {
        let mut file = archive.by_index(i).map_err(|e| e.to_string())?;
        if file.is_dir() {
            continue;
        }
        let name = file.name().to_string();
        let leaf = name.rsplit('/').next().unwrap_or(&name);
        if leaf.eq_ignore_ascii_case("duckdb") || leaf.eq_ignore_ascii_case("duckdb.exe") {
            copy_atomic(&mut file, &target)?;
            extracted = true;
            break;
        }
    }
    if !extracted {
        return Err("DuckDB binary not found inside the downloaded archive".to_string());
    }
    Ok(target)
}

#[derive(Debug, Serialize)]
pub struct EngineStatus {
    pub id: String,
    pub name: String,
    pub description: String,
    pub required: bool,
    pub installed: bool,
    /// The version currently on disk (None when no binary is present).
    pub version: Option<String>,
    /// The version this build of Duckle pins / ships. The UI compares it to
    /// `version` to offer an upgrade rather than a fresh install.
    pub target_version: String,
    /// A binary is present but its version differs from `target_version`, i.e.
    /// an upgrade is available (distinct from a missing engine).
    pub outdated: bool,
    pub path: Option<String>,
    pub available: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum InstallProgress {
    Downloading { received: u64, total: Option<u64> },
    Extracting,
    Verifying,
    /// Per-extension progress for the DuckDB extension pre-install step
    /// that runs after the engine binary lands. Fetching them up front
    /// means the first time a fresh user touches a Postgres source or an
    /// S3 file there is no network hop.
    InstallingExtension { name: String, index: u32, total: u32 },
    /// Model-file download phase, used only by the llamacpp engine.
    /// The model is much larger than the binary (~1.1 GB vs ~50 MB)
    /// so we report its progress separately for clearer UX.
    DownloadingModel { received: u64, total: Option<u64> },
    Done { path: String },
}

/// DuckDB extensions Duckle uses or is wired to use. Pre-installed once
/// at first launch so future ATTACH / read_xlsx / httpfs calls do not
/// stop to download an extension mid-run.
const DUCKDB_EXTENSIONS: &[&str] = &[
    "httpfs",   // S3 / GCS / HTTP(S) URLs
    "azure",    // Azure Blob native
    "sqlite",   // SQLite ATTACH
    "postgres", // PostgreSQL ATTACH
    "mysql",    // MySQL / MariaDB ATTACH
    "excel",    // .xlsx reader
    "iceberg",  // Apache Iceberg table scan + write (v1.5+)
    "delta",    // Delta Lake table scan
    "ducklake", // DuckLake: DuckDB-native lakehouse catalog
    "vss",      // Vector similarity search (array_* distance funcs)
    "fts",      // Full-text search (BM25 keyword scoring)
    // The avro community extension hasn't published for v1.4+ yet; src.avro
    // is marked preview in the palette until it catches up.
];

fn duckdb_command(bin: &Path) -> std::process::Command {
    let mut cmd = std::process::Command::new(bin);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        // CREATE_NO_WINDOW: suppress the console flash on Windows.
        cmd.creation_flags(0x0800_0000);
    }
    // -no-init: never process a user's ~/.duckdbrc. An init file that prints
    // output would pollute version/platform probes and extension installs.
    cmd.arg("-no-init");
    cmd
}

/// #91: ask the DuckDB binary its actual version (it prints e.g.
/// "v1.5.4 19864453f7" on the first line). Only duckdb is assumed to support
/// `--version` reliably. Used as a fallback when the install stamp is
/// missing/stale so a genuine pinned-version binary - placed by an older build,
/// an in-app self-update, or a manual drop - is not falsely flagged outdated.
fn probed_version(bin: &Path, s: &EngineSpec) -> Option<String> {
    if s.id != "duckdb" {
        return None;
    }
    let out = duckdb_command(bin).arg("--version").output().ok()?;
    if !out.status.success() {
        return None;
    }
    String::from_utf8_lossy(&out.stdout)
        .split_whitespace()
        .next()
        .map(|t| t.trim_start_matches('v').to_string())
        .filter(|v| !v.is_empty())
}

/// Walk through every DuckDB extension Duckle needs, INSTALL+LOADing each
/// so the file lands in the user's local DuckDB extension cache. Failures
/// are logged via the progress callback but never abort the engine
/// install: a user offline for one extension still gets a working engine
/// and the rest of the extensions; the missing one will autoload (or
/// fail loudly) the first time it's actually used.
fn install_duckdb_extensions<F: FnMut(InstallProgress)>(bin: &Path, on_progress: &mut F) {
    let total = DUCKDB_EXTENSIONS.len() as u32;
    for (i, ext) in DUCKDB_EXTENSIONS.iter().enumerate() {
        on_progress(InstallProgress::InstallingExtension {
            name: (*ext).to_string(),
            index: (i as u32) + 1,
            total,
        });
        let sql = format!("INSTALL {ext}; LOAD {ext};");
        // Best-effort: ignore the result; the next step (or a later run)
        // will retry. Don't let one slow / unreachable extension block
        // the whole engine install.
        let _ = duckdb_command(bin)
            .arg(":memory:")
            .arg("-c")
            .arg(&sql)
            .output();
    }
}

pub fn status(app_data: &Path) -> Vec<EngineStatus> {
    ENGINES
        .iter()
        .map(|s| {
            let path = binary_path(app_data, s);
            let exists = path.exists();
            let on_disk = installed_version(app_data, s);
            // #91: trust the install stamp as the fast path, but when it is
            // absent/stale fall back to the binary's own reported version, so a
            // genuine pinned-version binary without a stamp is not falsely
            // flagged outdated (the spurious "upgrade DuckDB 1.5.4" banner).
            let effective = if on_disk.is_some() {
                on_disk.clone()
            } else if exists {
                probed_version(&path, s)
            } else {
                None
            };
            // Backfill the stamp when the probe confirms the pinned version, so
            // subsequent calls hit the fast path and skip re-spawning the binary.
            if exists
                && on_disk.as_deref() != Some(s.version)
                && effective.as_deref() == Some(s.version)
            {
                let _ = std::fs::write(version_stamp_path(app_data, s), s.version);
            }
            // "installed" requires the binary to exist AND match the pinned
            // version, so bumping a version re-triggers the install flow.
            let installed = exists && effective.as_deref() == Some(s.version);
            // A binary is present but a different version: an upgrade is due.
            let outdated = exists && effective.as_deref() != Some(s.version);
            EngineStatus {
                id: s.id.to_string(),
                name: s.name.to_string(),
                description: s.description.to_string(),
                required: s.required,
                installed,
                // Report the real on-disk version when a binary is present
                // (so the UI shows the outdated version, not the pinned one).
                version: if exists { effective } else { None },
                target_version: s.version.to_string(),
                outdated,
                path: exists.then(|| path.to_string_lossy().to_string()),
                available: asset_for(s).is_some(),
            }
        })
        .collect()
}

/// Download + install any engine by id. Streams progress.
/// `model_id` picks the GGUF for the chat assistant; it is ignored by every
/// other engine, and None means the default model.
pub fn install<F: FnMut(InstallProgress)>(
    app_data: &Path,
    engine_id: &str,
    model_id: Option<&str>,
    on_progress: F,
) -> Result<String, String> {
    let s = spec(engine_id).ok_or_else(|| format!("Unknown engine '{}'", engine_id))?;
    install_spec(app_data, s, model_id, on_progress)
}

/// macOS (#89): make a freshly downloaded engine dir launchable. Downloaded
/// llama.cpp release binaries are signed by ggml's identity but not notarized;
/// on macOS 15+ the leftover `com.apple.quarantine` / `com.apple.provenance`
/// xattrs plus the non-notarized signature get the process SIGKILL'd on exec,
/// which surfaces as the llama-server "didn't become ready" timeout. Clearing
/// ALL xattrs (not just quarantine - `com.apple.provenance` also gates
/// Gatekeeper re-evaluation) and ad-hoc re-signing the Mach-O files (dylibs
/// first, then the executable, with no hardened-runtime flag so library
/// validation won't reject the ad-hoc dylibs) makes them launchable. Writes a
/// marker so `ensure_macos_launchable` can skip the work on later launches. Only
/// the main binary failing to sign is fatal - it would not run at all. No-op off
/// macOS (guarded at run time so the code still type-checks on every platform).
pub(crate) fn macos_prepare_engine(dir: &Path, main_binary: &Path) -> Result<(), String> {
    if !cfg!(target_os = "macos") {
        return Ok(());
    }
    let _ = std::process::Command::new("xattr")
        .arg("-cr")
        .arg(dir)
        .output();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.extension().map(|x| x == "dylib").unwrap_or(false) {
                let _ = adhoc_sign(&p);
            }
        }
    }
    adhoc_sign(main_binary)?;
    let _ = std::fs::write(dir.join(MACOS_PREPARED_MARKER), "1");
    Ok(())
}

/// macOS (#89): ad-hoc code-sign one Mach-O file. `--sign -` produces a local
/// signature with no hardened-runtime flag, so the file runs on this machine
/// without notarization and won't enforce library validation against a team id.
fn adhoc_sign(path: &Path) -> Result<(), String> {
    let out = std::process::Command::new("codesign")
        .args(["--force", "--sign", "-", "--timestamp=none"])
        .arg(path)
        .output()
        .map_err(|e| format!("run codesign on {}: {}", path.display(), e))?;
    if out.status.success() {
        Ok(())
    } else {
        Err(format!(
            "codesign {}: {}",
            path.display(),
            String::from_utf8_lossy(&out.stderr).trim()
        ))
    }
}

const MACOS_PREPARED_MARKER: &str = ".duckle-macos-prepared";

/// macOS (#89): ensure an already-installed engine dir was made launchable,
/// repairing installs done by a Duckle build that predated the signing fix (or
/// one whose install-time signing was too weak). Marker-gated so it does the
/// clear + re-sign at most once per install. No-op off macOS. Call this right
/// before launching a downloaded binary.
pub(crate) fn ensure_macos_launchable(dir: &Path, main_binary: &Path) -> Result<(), String> {
    if !cfg!(target_os = "macos") || dir.join(MACOS_PREPARED_MARKER).exists() {
        return Ok(());
    }
    macos_prepare_engine(dir, main_binary)
}

/// Extract a .tar.gz engine archive into `dir`, flattening every entry to its
/// leaf name so the binary keeps its sibling shared libraries. Returns whether
/// the wanted binary was found (it is written atomically to `target`, since
/// status() keys off that path existing).
///
/// Link entries need real handling. A tar stores a symlink's target in the
/// HEADER with an empty body, so copying the entry writes a 0-byte regular
/// file. That was issue #89: llama.cpp ships `libllama.0.0.9305.dylib` plus the
/// `libllama.0.dylib` / `libllama.dylib` symlinks its binaries actually link
/// against, and those landed as empty files. dyld then reported
/// `Library not loaded: @rpath/libllama-common.0.dylib ... tried: '<path>' ()`,
/// where the empty parentheses mean the file was found but was not a valid
/// Mach-O. No amount of re-signing could fix that, because there was nothing to
/// sign; the bytes were never written.
fn extract_tar_gz(
    buf: &[u8],
    dir: &Path,
    target: &Path,
    want: &str,
    binary: &str,
) -> Result<bool, String> {
    let gz = flate2::read::GzDecoder::new(std::io::Cursor::new(buf));
    let mut archive = tar::Archive::new(gz);
    let mut extracted = false;
    // Links are replayed after the regular files, because a tar may list a
    // link before the entry it points at.
    let mut links: Vec<(String, String)> = Vec::new();
    for entry in archive.entries().map_err(|e| e.to_string())? {
        let mut entry = entry.map_err(|e| e.to_string())?;
        let path = entry.path().map_err(|e| e.to_string())?.to_path_buf();
        let leaf = path
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();
        if entry.header().entry_type().is_dir() || leaf.is_empty() {
            continue;
        }
        let et = entry.header().entry_type();
        if et.is_symlink() || et.is_hard_link() {
            let target_leaf = entry
                .link_name()
                .ok()
                .flatten()
                .and_then(|p| p.file_name().map(|f| f.to_string_lossy().to_string()))
                .unwrap_or_default();
            if !target_leaf.is_empty() && target_leaf != leaf {
                links.push((leaf, target_leaf));
            }
            continue;
        }
        let is_target_binary =
            leaf.eq_ignore_ascii_case(want) || leaf.eq_ignore_ascii_case(binary);
        if is_target_binary {
            // Atomic for the binary status() keys off of.
            copy_atomic(&mut entry, target)?;
            extracted = true;
        } else {
            let out_path = dir.join(&leaf);
            let mut out = std::fs::File::create(&out_path).map_err(|e| e.to_string())?;
            std::io::copy(&mut entry, &mut out).map_err(|e| e.to_string())?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ =
                    std::fs::set_permissions(&out_path, std::fs::Permissions::from_mode(0o755));
            }
        }
    }
    // Recreate the links now every real file is on disk. All of llama.cpp's
    // links are same-directory, so the flattened leaf is a valid relative
    // target.
    for (link_leaf, target_leaf) in links {
        let link_path = dir.join(&link_leaf);
        if !dir.join(&target_leaf).exists() {
            // Nothing to point at: leave what is there rather than replacing a
            // real file with a dangling link.
            continue;
        }
        let _ = std::fs::remove_file(&link_path);
        #[cfg(unix)]
        {
            if let Err(e) = std::os::unix::fs::symlink(&target_leaf, &link_path) {
                return Err(format!(
                    "could not link {} -> {}: {}",
                    link_leaf, target_leaf, e
                ));
            }
        }
        #[cfg(not(unix))]
        {
            // Windows needs a privilege for symlinks, so copy instead. The
            // duplicated bytes are harmless and the loader is satisfied.
            std::fs::copy(dir.join(&target_leaf), &link_path).map_err(|e| {
                format!("could not copy {} -> {}: {}", target_leaf, link_leaf, e)
            })?;
        }
    }
    Ok(extracted)
}

fn install_spec<F: FnMut(InstallProgress)>(
    app_data: &Path,
    s: &EngineSpec,
    model_id: Option<&str>,
    mut on_progress: F,
) -> Result<String, String> {
    let asset = asset_for(s).ok_or_else(|| {
        format!(
            "No {} build for {}-{}",
            s.name,
            std::env::consts::OS,
            std::env::consts::ARCH
        )
    })?;
    // Tag naming convention varies per upstream: DuckDB + SlothDB
    // both use v-prefixed semver tags (v1.5.3); llama.cpp uses raw
    // build tags (b9305). Pre-prepending `v` to every version
    // produces a 404 against ggml-org/llama.cpp.
    let tag = if s.id == "llamacpp" {
        s.version.to_string()
    } else {
        format!("v{}", s.version)
    };
    let url = format!(
        "https://github.com/{}/releases/download/{}/{}",
        s.repo, tag, asset
    );

    let dir = engine_dir(app_data, s);
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;

    let client = reqwest::blocking::Client::builder()
        .user_agent("duckle")
        // Trust the OS store (+ optional DUCKLE_CA_CERT) on top of the bundled
        // roots so the engine download works behind a TLS-inspecting proxy.
        .use_preconfigured_tls(duckle_duckdb_engine::tls::build_client_config())
        .build()
        .map_err(|e| e.to_string())?;
    let mut resp = client.get(&url).send().map_err(|e| e.to_string())?;
    if !resp.status().is_success() {
        return Err(format!(
            "Couldn't download {} (HTTP {}). The release {} may not exist yet.",
            s.name,
            resp.status().as_u16(),
            s.version
        ));
    }
    let total = resp.content_length();
    let mut buf: Vec<u8> = Vec::with_capacity(total.unwrap_or(0) as usize);
    let mut chunk = [0u8; 64 * 1024];
    let mut received: u64 = 0;
    on_progress(InstallProgress::Downloading { received: 0, total });
    loop {
        let n = resp.read(&mut chunk).map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        received += n as u64;
        on_progress(InstallProgress::Downloading { received, total });
    }

    // Before extraction. llama.cpp's archive yields a server binary plus the shared
    // libraries it dlopens, so a substituted archive is arbitrary code either way.
    verify_download(
        &format!("the {} download", s.name),
        &hex_sha256(&buf),
        expected_engine_sha256(s.id, s.version),
    )?;

    let target = binary_path(app_data, s);

    let lower = asset.to_ascii_lowercase();
    if lower.ends_with(".zip") {
        on_progress(InstallProgress::Extracting);
        let want = binary_file_name(s);
        let reader = std::io::Cursor::new(buf);
        let mut archive = zip::ZipArchive::new(reader).map_err(|e| e.to_string())?;
        let mut extracted = false;
        // llama.cpp's zip ships the server binary alongside several
        // shared libraries (llama.dll, ggml.dll, ...) that the binary
        // dlopens at runtime - we have to extract them too. DuckDB
        // ships a single self-contained binary; the targeted extract
        // path stays for it.
        let extract_all = s.id == "llamacpp";
        for i in 0..archive.len() {
            let mut file = archive.by_index(i).map_err(|e| e.to_string())?;
            let name = file.name().to_string();
            let leaf = name.rsplit('/').next().unwrap_or(&name).to_string();
            if file.is_dir() || leaf.is_empty() {
                continue;
            }
            let is_target_binary =
                leaf.eq_ignore_ascii_case(&want) || leaf.eq_ignore_ascii_case(s.binary);
            if extract_all {
                if is_target_binary {
                    // Write the binary status() keys off of atomically so a
                    // crash mid-extract can't leave a partial "installed" file.
                    copy_atomic(&mut file, &target)?;
                    extracted = true;
                } else {
                    let out_path = dir.join(&leaf);
                    let mut out =
                        std::fs::File::create(&out_path).map_err(|e| e.to_string())?;
                    std::io::copy(&mut file, &mut out).map_err(|e| e.to_string())?;
                }
            } else if is_target_binary {
                copy_atomic(&mut file, &target)?;
                extracted = true;
                break;
            }
        }
        if !extracted {
            return Err(format!(
                "{} binary not found inside the downloaded archive",
                s.name
            ));
        }
    } else if lower.ends_with(".tar.gz") || lower.ends_with(".tgz") {
        // llama.cpp's Linux + macOS releases ship as tar.gz. Same
        // semantics as the llamacpp zip branch: extract every file
        // to the engine dir so the binary keeps its sibling .so / .dylib.
        on_progress(InstallProgress::Extracting);
        let want = binary_file_name(s);
        let extracted = extract_tar_gz(&buf, &dir, &target, &want, s.binary)?;
        if !extracted {
            return Err(format!(
                "{} binary not found inside the downloaded tarball",
                s.name
            ));
        }
    } else {
        // Raw single-file binary (SlothDB) - the download IS the binary.
        if buf.is_empty() {
            return Err(format!("{} download was empty", s.name));
        }
        // Reject a truncated transfer, then install atomically so a partial
        // binary never lands at the final path (status() would call it
        // installed).
        if let Some(t) = total {
            if (buf.len() as u64) < t {
                return Err(format!(
                    "{} download truncated ({} of {} bytes); try again",
                    s.name,
                    buf.len(),
                    t
                ));
            }
        }
        write_atomic(&target, &buf)?;
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o755));
    }

    // macOS (#89): clear Gatekeeper xattrs + ad-hoc re-sign the freshly
    // downloaded binaries so the kernel allows execution. A broken signature on
    // llama-server only surfaces later as an opaque "didn't become ready"
    // timeout, so it is fatal here; for the self-contained DB engines it stays
    // best-effort (they run regardless).
    if let Err(e) = macos_prepare_engine(&dir, &target) {
        if s.id == "llamacpp" {
            return Err(format!(
                "Installed {} but could not sign it for macOS: {}",
                s.name, e
            ));
        }
    }

    // Verify the binary landed and is non-empty. Probing --version is
    // best-effort: DuckDB supports it; we don't assume every engine does,
    // so a non-zero --version isn't fatal as long as the file is there.
    on_progress(InstallProgress::Verifying);
    let bytes = std::fs::metadata(&target).map(|m| m.len()).unwrap_or(0);
    if bytes == 0 {
        return Err(format!("Installed {} binary is empty", s.name));
    }
    let _ = duckdb_command(&target).arg("--version").output();

    // Stamp the installed version so status() detects a future version bump
    // and re-installs instead of keeping a stale binary.
    let _ = std::fs::write(version_stamp_path(app_data, s), s.version);

    // The host binary above was overwritten in place, but a version bump also
    // leaves the previous version's cached cross-OS DuckDB binaries stale
    // (engines/duckdb-cross/, used by Build Pipeline). They are NOT version-
    // keyed and short-circuit on existence, so without this they would never be
    // re-fetched. Drop the whole cache so the next Build Pipeline downloads the
    // matching version - and so an old (e.g. 1.5.3) binary is not left behind
    // in the app storage directory. Best-effort.
    if s.id == "duckdb" {
        let cross = app_data.join("engines").join("duckdb-cross");
        let _ = std::fs::remove_dir_all(&cross);
    }

    // Pre-fetch the extensions Duckle uses so the first connector hit
    // doesn't pause to download an extension. Only meaningful for the
    // DuckDB engine; SlothDB has its own model.
    if s.id == "duckdb" {
        install_duckdb_extensions(&target, &mut on_progress);
    }

    // llama.cpp's binary alone is useless without a model. Fetch the chosen
    // GGUF from HuggingFace right after the binary lands.
    if s.id == "llamacpp" {
        install_llama_model(app_data, model_id, &mut on_progress)?;
    }

    let path = target.to_string_lossy().to_string();
    on_progress(InstallProgress::Done { path: path.clone() });
    Ok(path)
}

/// A unique temp sibling of `target` for atomic download/extract: write here,
/// then rename into place so a truncated / crash-interrupted file never appears
/// at the final path (where status()/idempotency checks would treat it as
/// installed).
fn part_path(target: &Path) -> PathBuf {
    let mut name = target
        .file_name()
        .map(|n| n.to_os_string())
        .unwrap_or_default();
    name.push(format!(".part{}", std::process::id()));
    target.with_file_name(name)
}

/// Rename a fully-written temp file into `target` (exec perms on unix first);
/// removes the temp on failure so a partial never lingers.
fn finalize_download(tmp: &Path, target: &Path) -> Result<(), String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(tmp, std::fs::Permissions::from_mode(0o755));
    }
    std::fs::rename(tmp, target).map_err(|e| {
        let _ = std::fs::remove_file(tmp);
        format!("finalize {}: {}", target.display(), e)
    })
}

/// Write `bytes` to a temp sibling, then rename into `target` atomically.
fn write_atomic(target: &Path, bytes: &[u8]) -> Result<(), String> {
    let tmp = part_path(target);
    if let Err(e) = std::fs::write(&tmp, bytes) {
        let _ = std::fs::remove_file(&tmp);
        return Err(e.to_string());
    }
    finalize_download(&tmp, target)
}

/// Copy a reader to a temp sibling, then rename into `target` atomically.
fn copy_atomic(reader: &mut impl std::io::Read, target: &Path) -> Result<(), String> {
    let tmp = part_path(target);
    let res = (|| -> Result<(), String> {
        let mut out = std::fs::File::create(&tmp).map_err(|e| e.to_string())?;
        std::io::copy(reader, &mut out).map_err(|e| e.to_string())?;
        Ok(())
    })();
    if let Err(e) = res {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    finalize_download(&tmp, target)
}

/// Download the chosen GGUF model file into the llamacpp engine dir.
/// Separate phase from the binary download so the UI can show "stage
/// 2 of 2" instead of one big progress bar for both. HuggingFace
/// supports range requests; we just stream sequentially for simplicity.
///
/// `model_id` is the id the user picked at install time; an unknown or absent
/// one falls back to the default rather than failing.
fn install_llama_model<F: FnMut(InstallProgress)>(
    app_data: &Path,
    model_id: Option<&str>,
    on_progress: &mut F,
) -> Result<(), String> {
    let model = llama_model(model_id);
    // Target by exact filename, not by whatever .gguf happens to be present:
    // picking a different model must download it, not silently keep the old one.
    let target = engine_dir(app_data, &LLAMACPP).join(model.file);
    // Idempotent: if this model is already there and non-empty, skip.
    if let Ok(meta) = std::fs::metadata(&target) {
        if meta.len() > 1_000_000 {
            return Ok(());
        }
    }
    let url = format!(
        "https://huggingface.co/{}/resolve/main/{}",
        model.repo, model.file
    );
    let client = reqwest::blocking::Client::builder()
        .user_agent("duckle")
        // No global timeout - the model is over a GB on home internet.
        .timeout(None)
        // Same merged trust store as the engine download (OS + bundled roots).
        .use_preconfigured_tls(duckle_duckdb_engine::tls::build_client_config())
        .build()
        .map_err(|e| e.to_string())?;
    let mut resp = client.get(&url).send().map_err(|e| e.to_string())?;
    if !resp.status().is_success() {
        return Err(format!(
            "Couldn't download Qwen model (HTTP {}). HuggingFace may be rate-limiting; try again in a minute.",
            resp.status().as_u16()
        ));
    }
    let total = resp.content_length();
    on_progress(InstallProgress::DownloadingModel { received: 0, total });
    // Stream to a temp sibling, validate, then rename into place - so a
    // truncated or interrupted download never lands at the model path where the
    // idempotency check above would treat it as fully installed.
    let tmp = part_path(&target);
    let mut out = std::fs::File::create(&tmp).map_err(|e| e.to_string())?;
    let mut chunk = [0u8; 256 * 1024];
    let mut received: u64 = 0;
    // Hashed as it streams: the file is over a gigabyte, so it is never held in
    // memory and cannot be hashed afterwards without reading it back.
    let mut hasher = <sha2::Sha256 as sha2::Digest>::new();
    let validated = (|| -> Result<(), String> {
        loop {
            let n = resp.read(&mut chunk).map_err(|e| e.to_string())?;
            if n == 0 {
                break;
            }
            sha2::Digest::update(&mut hasher, &chunk[..n]);
            std::io::Write::write_all(&mut out, &chunk[..n]).map_err(|e| e.to_string())?;
            received += n as u64;
            on_progress(InstallProgress::DownloadingModel { received, total });
        }
        std::io::Write::flush(&mut out).map_err(|e| e.to_string())?;
        // Truncated transfer: the server declared more bytes than arrived.
        if let Some(t) = total {
            if received < t {
                return Err(format!(
                    "model download truncated ({} of {} bytes); try again",
                    received, t
                ));
            }
        }
        // GGUF files start with the magic bytes "GGUF".
        if received < 4 {
            return Err("model download too small to be a GGUF file".into());
        }
        let mut header = [0u8; 4];
        let mut f = std::fs::File::open(&tmp).map_err(|e| e.to_string())?;
        std::io::Read::read_exact(&mut f, &mut header)
            .map_err(|e| format!("read model header: {}", e))?;
        if &header != b"GGUF" {
            return Err("Downloaded model is not a valid GGUF file (header mismatch)".into());
        }
        // Inside `validated`, so a mismatch takes the existing path that deletes the
        // partial file. Leaving it on disk would be worse than not checking at all:
        // the next run finds the target present and skips the download entirely.
        let digest: String = sha2::Digest::finalize(hasher)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        verify_download(
            &format!("the {} model download", model.file),
            &digest,
            expected_model_sha256(model.repo, model.file),
        )?;
        Ok(())
    })();
    drop(out); // close the handle before rename (Windows)
    if let Err(e) = validated {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    finalize_download(&tmp, &target)
}

#[cfg(test)]
mod download_verification_tests {
    use super::*;

    /// A known vector, so a change to the hex formatting cannot pass unnoticed.
    #[test]
    fn hex_sha256_matches_a_known_vector() {
        assert_eq!(
            hex_sha256(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    /// The whole point: an artifact that does not match its pin must be refused,
    /// because the next thing that happens to it is execution.
    #[test]
    fn a_mismatched_digest_is_refused() {
        let real = hex_sha256(b"the real installer");
        let swapped = hex_sha256(b"the attacker's installer");
        let err = verify_download("test artifact", &swapped, &real)
            .expect_err("a substituted artifact must be refused");
        assert!(err.contains("failed checksum verification"), "unhelpful: {err}");

        verify_download("test artifact", &real, &real).expect("the real artifact must pass");
    }

    /// Digests get pasted from tools that emit upper case.
    #[test]
    fn digest_comparison_ignores_case() {
        let d = hex_sha256(b"x");
        verify_download("test artifact", &d.to_uppercase(), &d).unwrap();
    }

    /// An unpinned artifact still installs, because refusing every download until
    /// the table is filled in would break every install. Pinning one is what turns
    /// enforcement on for it, with no second switch to forget.
    #[test]
    fn an_unpinned_artifact_warns_rather_than_failing() {
        verify_download("test artifact", &hex_sha256(b"anything"), UNPINNED)
            .expect("an unpinned artifact must not block the install");
    }
}

#[cfg(test)]
mod tar_extract_tests {
    use super::extract_tar_gz;
    use std::io::Write;

    /// Build a .tar.gz shaped like llama.cpp's macOS release: a real versioned
    /// dylib, the two symlinks that point at it, and the server binary. The
    /// symlink entries are written explicitly so this holds on any host
    /// filesystem, including one that cannot create symlinks.
    fn llamacpp_shaped_tarball() -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());

        let real = b"FAKE-MACHO-DYLIB-BYTES";
        let mut h = tar::Header::new_gnu();
        h.set_path("build/bin/libllama-common.0.0.9305.dylib").unwrap();
        h.set_size(real.len() as u64);
        h.set_mode(0o755);
        h.set_entry_type(tar::EntryType::Regular);
        h.set_cksum();
        builder.append(&h, &real[..]).unwrap();

        for link in ["libllama-common.0.dylib", "libllama-common.dylib"] {
            let mut h = tar::Header::new_gnu();
            h.set_size(0); // a tar symlink carries no body
            h.set_mode(0o755);
            h.set_entry_type(tar::EntryType::Symlink);
            h.set_cksum();
            builder
                .append_link(&mut h, format!("build/bin/{link}"), "libllama-common.0.0.9305.dylib")
                .unwrap();
        }

        let server = b"FAKE-SERVER-BINARY";
        let mut h = tar::Header::new_gnu();
        h.set_path("build/bin/llama-server").unwrap();
        h.set_size(server.len() as u64);
        h.set_mode(0o755);
        h.set_entry_type(tar::EntryType::Regular);
        h.set_cksum();
        builder.append(&h, &server[..]).unwrap();

        let tar_bytes = builder.into_inner().unwrap();
        let mut gz =
            flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gz.write_all(&tar_bytes).unwrap();
        gz.finish().unwrap()
    }

    /// Issue #89. The dylib symlinks used to land as 0-byte regular files,
    /// because a tar symlink has an empty body and the extractor copied the
    /// body. dyld then refused to load them and llama-server aborted, which
    /// looked like a code-signing failure and was not.
    #[test]
    fn dylib_symlinks_are_not_extracted_as_empty_files() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        let target = dir.join("llama-server");
        let found =
            extract_tar_gz(&llamacpp_shaped_tarball(), dir, &target, "llama-server", "llama-server")
                .expect("extraction succeeds");
        assert!(found, "the wanted binary must be found");

        let real = dir.join("libllama-common.0.0.9305.dylib");
        assert_eq!(std::fs::read(&real).unwrap(), b"FAKE-MACHO-DYLIB-BYTES");

        for link in ["libllama-common.0.dylib", "libllama-common.dylib"] {
            let p = dir.join(link);
            assert!(p.exists(), "{link} must exist");
            let meta = std::fs::metadata(&p).unwrap(); // follows the link
            assert_ne!(
                meta.len(),
                0,
                "{link} resolved to 0 bytes, which is the #89 bug"
            );
            // Reading through the link must yield the real library's bytes,
            // whether it is a symlink (unix) or a copy (windows fallback).
            assert_eq!(
                std::fs::read(&p).unwrap(),
                b"FAKE-MACHO-DYLIB-BYTES",
                "{link} must resolve to the real dylib"
            );
        }
    }

    /// A link whose target is missing must not clobber anything or error the
    /// whole install.
    #[test]
    fn dangling_link_is_skipped_not_fatal() {
        let mut builder = tar::Builder::new(Vec::new());
        let mut h = tar::Header::new_gnu();
        h.set_size(0);
        h.set_mode(0o755);
        h.set_entry_type(tar::EntryType::Symlink);
        h.set_cksum();
        builder
            .append_link(&mut h, "bin/orphan.dylib", "nothing-here.dylib")
            .unwrap();
        let bin = b"BINARY";
        let mut h = tar::Header::new_gnu();
        h.set_path("bin/llama-server").unwrap();
        h.set_size(bin.len() as u64);
        h.set_entry_type(tar::EntryType::Regular);
        h.set_cksum();
        builder.append(&h, &bin[..]).unwrap();
        let tar_bytes = builder.into_inner().unwrap();
        let mut gz =
            flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gz.write_all(&tar_bytes).unwrap();
        let buf = gz.finish().unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("llama-server");
        let found = extract_tar_gz(&buf, tmp.path(), &target, "llama-server", "llama-server")
            .expect("a dangling link must not fail the install");
        assert!(found);
        assert!(!tmp.path().join("orphan.dylib").exists());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_lists_all_engines_missing_in_empty_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let st = status(tmp.path());
        assert_eq!(st.len(), 3);
        let duck = st.iter().find(|e| e.id == "duckdb").unwrap();
        assert!(!duck.installed && duck.required && duck.available);
        let sloth = st.iter().find(|e| e.id == "slothdb").unwrap();
        assert!(!sloth.installed && !sloth.required);
        let llama = st.iter().find(|e| e.id == "llamacpp").unwrap();
        assert!(!llama.installed && !llama.required);
    }

    #[test]
    fn status_flags_outdated_when_stamp_differs() {
        // An existing user on an older DuckDB: the binary is present but the
        // version stamp differs from the pinned one. It must read as outdated
        // (upgrade available) and NOT installed, with both versions exposed so
        // the UI can prompt an upgrade rather than a fresh install.
        let tmp = tempfile::tempdir().unwrap();
        let dir = engine_dir(tmp.path(), &DUCKDB);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(binary_file_name(&DUCKDB)), b"old-binary").unwrap();
        std::fs::write(dir.join(".installed-version"), "0.0.1-old").unwrap();

        let st = status(tmp.path());
        let duck = st.iter().find(|e| e.id == "duckdb").unwrap();
        assert!(!duck.installed, "an old version must not read as installed");
        assert!(duck.outdated, "an old version must read as outdated");
        assert_eq!(duck.version.as_deref(), Some("0.0.1-old"));
        assert_eq!(duck.target_version, DUCKDB.version);
    }

    #[test]
    #[ignore = "downloads the DuckDB CLI from GitHub releases (network)"]
    fn installs_duckdb() {
        let tmp = tempfile::tempdir().unwrap();
        let path = install(tmp.path(), "duckdb", None, |_| {}).expect("install");
        assert!(std::path::Path::new(&path).exists());
        assert!(status(tmp.path())
            .iter()
            .any(|e| e.id == "duckdb" && e.installed));
    }

    #[test]
    #[ignore = "downloads the SlothDB raw binary from GitHub releases (network)"]
    fn installs_slothdb() {
        let tmp = tempfile::tempdir().unwrap();
        let path = install(tmp.path(), "slothdb", None, |_| {}).expect("install");
        let p = std::path::Path::new(&path);
        assert!(p.exists(), "binary should exist");
        assert!(
            std::fs::metadata(p).unwrap().len() > 0,
            "binary should be non-empty"
        );
        assert!(status(tmp.path())
            .iter()
            .any(|e| e.id == "slothdb" && e.installed));
    }
}
