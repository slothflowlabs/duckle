//! GPU acceleration through NVIDIA Sirius.
//!
//! Sirius is a DuckDB core extension, from DuckDB 2.0.1, that runs the queries
//! it supports on an NVIDIA GPU and leaves the rest on DuckDB's CPU engine.
//! DuckDB builds it for linux_amd64 and linux_arm64 only, and it needs a Turing
//! or newer GPU with a driver for CUDA 12.8 or newer. The first `LOAD` downloads
//! the GPU backend that matches the driver.
//!
//! `DUCKLE_GPU` chooses: `auto`, the default, loads Sirius for a run wherever it
//! loads; `on` fails a run that cannot have it; `off` never loads it.
//!
//! What Duckle puts around the extension, each from a measurement on DuckDB
//! 2.0.1 (RTX 4050 Laptop, CUDA 13.1, Linux container):
//!
//! - **A check before the first run.** `LOAD sirius` falls back to the CPU when
//!   it finds no driver, but fails outright when the backend starts and then
//!   cannot initialise. By default it does exactly that wherever the host
//!   reports no NUMA node (WSL2, many VMs and containers), because it sizes its
//!   pinned host memory from that node's RAM. Each DuckDB binary is checked once
//!   per process, so a machine where Sirius cannot start runs on the CPU rather
//!   than failing every run.
//! - **A config of its own**, passed as `SIRIUS_CONFIG_FILE`, with absolute
//!   memory budgets in place of NUMA fractions. One the user provides (that
//!   variable, `./sirius.yaml` or `~/.sirius/sirius.yaml`) is left to win.
//! - **One GPU run at a time, machine-wide.** Sirius claims most of the GPU when
//!   it starts, so a second process on the same card may not start at all. A
//!   run that cannot take the GPU lock runs on the CPU, or under `on` fails
//!   saying why.
//! - **A line in the run log** saying whether the run had the GPU.
//!
//! Loading Sirius adds about 1.2 s to each DuckDB process that loads it (0.04 s
//! without), so a run that executes stage by stage pays it once per stage.

use std::collections::HashMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Mutex, OnceLock};

/// What `DUCKLE_GPU` asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GpuMode {
    Auto,
    On,
    Off,
}

impl GpuMode {
    pub fn as_str(self) -> &'static str {
        match self {
            GpuMode::Auto => "auto",
            GpuMode::On => "on",
            GpuMode::Off => "off",
        }
    }
}

/// Read a mode as a person typed it. Empty means the default.
pub fn parse_mode(raw: &str) -> Result<GpuMode, String> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "" | "auto" => Ok(GpuMode::Auto),
        "on" => Ok(GpuMode::On),
        "off" => Ok(GpuMode::Off),
        other => Err(format!(
            "GPU acceleration must be auto, on or off, not '{other}'"
        )),
    }
}

/// The mode this process runs with, from `DUCKLE_GPU`.
pub fn mode() -> Result<GpuMode, String> {
    parse_mode(&std::env::var("DUCKLE_GPU").unwrap_or_default())
}

/// Why Sirius cannot exist on this platform, or `None` where DuckDB builds it.
fn platform_reason() -> Option<String> {
    let supported = cfg!(all(
        target_os = "linux",
        target_env = "gnu",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ));
    if supported {
        return None;
    }
    Some(format!(
        "DuckDB builds Sirius for Linux on x86-64 and arm64 only, and this is {} on {}",
        std::env::consts::OS,
        std::env::consts::ARCH
    ))
}

/// Is an NVIDIA driver installed where Sirius will look for it?
///
/// Sirius opens `libcuda.so.1`. A native driver also publishes
/// `/proc/driver/nvidia/version`; WSL2 puts the library under `/usr/lib/wsl/lib`;
/// a container given the GPU has it in the loader cache; a hand-placed one is
/// found through `LD_LIBRARY_PATH`. Any of them is enough to try.
fn driver_found_in(
    proc_version: &Path,
    wsl_lib: &Path,
    ld_cache: &Path,
    ld_library_path: &str,
) -> bool {
    if proc_version.exists() || wsl_lib.join("libcuda.so.1").exists() {
        return true;
    }
    if std::fs::read(ld_cache).is_ok_and(|bytes| bytes.windows(12).any(|w| w == b"libcuda.so.1")) {
        return true;
    }
    std::env::split_paths(ld_library_path).any(|dir| dir.join("libcuda.so.1").exists())
}

fn driver_found() -> bool {
    driver_found_in(
        Path::new("/proc/driver/nvidia/version"),
        Path::new("/usr/lib/wsl/lib"),
        Path::new("/etc/ld.so.cache"),
        &std::env::var("LD_LIBRARY_PATH").unwrap_or_default(),
    )
}

/// Pinned host memory for Sirius: a quarter of RAM, at least 1 GiB.
///
/// Sirius's own default is 90% of each NUMA node, which it cannot size where
/// there is no NUMA node, and which beside DuckDB's own memory limit (80% of
/// RAM by default) would promise the same memory twice. Pinned memory cannot be
/// swapped out, so this stays well clear of what the rest of the run needs.
fn host_budget(meminfo: &str) -> u64 {
    const GIB: u64 = 1024 * 1024 * 1024;
    let total_kib = meminfo
        .lines()
        .find_map(|l| l.strip_prefix("MemTotal:"))
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|n| n.parse::<u64>().ok());
    match total_kib {
        Some(kib) => (kib * 1024 / 4).max(GIB),
        None => GIB,
    }
}

/// The config Duckle hands Sirius when the user has none.
fn config_text(host_bytes: u64) -> String {
    format!(
        "# Written by Duckle for the DuckDB processes it starts with NVIDIA Sirius.\n\
         # To use your own, put a sirius.yaml in ~/.sirius/ or set SIRIUS_CONFIG_FILE.\n\
         sirius:\n  \
           memory:\n    \
             gpu:\n      \
               usage_limit_fraction: {GPU_FRACTION}\n    \
             host:\n      \
               capacity_bytes: {host_bytes}\n      \
               initial_number_pools: 1\n"
    )
}

/// Share of the GPU's memory Sirius may claim. Sirius's own default is 0.95,
/// which leaves a desktop's display, and anything else on the card, 5%.
const GPU_FRACTION: &str = "0.8";

/// A config the user set up for Sirius, which Duckle then leaves alone.
fn user_config_present(home: Option<&Path>, cwd: Option<&Path>) -> bool {
    std::env::var_os("SIRIUS_CONFIG_FILE").is_some_and(|v| !v.is_empty())
        || cwd.is_some_and(|d| d.join("sirius.yaml").exists())
        || home.is_some_and(|h| h.join(".sirius").join("sirius.yaml").exists())
}

/// The config Duckle wrote this process, once a check needed one.
static MANAGED_CONFIG: OnceLock<Option<PathBuf>> = OnceLock::new();

/// The config file Duckle points its DuckDB processes at, written on first use.
///
/// `None` when the user has a config of their own, or when this platform cannot
/// run Sirius at all.
fn managed_config() -> Option<PathBuf> {
    MANAGED_CONFIG
        .get_or_init(|| {
            if platform_reason().is_some() {
                return None;
            }
            let home = std::env::var_os("HOME").map(PathBuf::from);
            let cwd = std::env::current_dir().ok();
            if user_config_present(home.as_deref(), cwd.as_deref()) {
                return None;
            }
            let meminfo = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
            let dir = std::env::temp_dir().join("duckle-sirius");
            let path = dir.join("sirius.yaml");
            std::fs::create_dir_all(&dir).ok()?;
            std::fs::write(&path, config_text(host_budget(&meminfo))).ok()?;
            Some(path)
        })
        .clone()
}

/// The `SIRIUS_CONFIG_FILE` a DuckDB child should get: the config Duckle wrote,
/// once a GPU check in this process has. Never written just to be passed on, so
/// a machine that never checks never gets the file.
pub(crate) fn config_for_children() -> Option<PathBuf> {
    MANAGED_CONFIG.get().cloned().flatten()
}

/// What `sirius_status()` reports, read back from the check.
#[derive(Debug, Clone, PartialEq, Default, serde::Deserialize)]
struct SiriusStatus {
    #[serde(default)]
    driver_found: bool,
    #[serde(default)]
    cuda_driver_version: Option<String>,
    #[serde(default)]
    backend: Option<String>,
    #[serde(default)]
    backend_loaded: bool,
    #[serde(default)]
    gpu_execution: bool,
    #[serde(default)]
    fallback_reason: Option<String>,
}

/// Whether Sirius can be loaded here, as one check found it.
#[derive(Debug, Clone, PartialEq)]
pub enum Probe {
    /// Sirius loaded and took over execution. `caveat` is set when it loaded
    /// but a trial query still ran on the CPU, with Sirius's reason.
    Ready {
        backend: String,
        cuda: String,
        caveat: Option<String>,
    },
    /// It cannot be loaded here, and this is why.
    Unavailable(String),
}

/// What the check may fetch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fetch {
    Allowed,
    /// `DUCKLE_OFFLINE`: nothing is downloaded, anything installed loads.
    Offline,
    /// A network policy: the same settings every run under it starts with.
    PolicyDenied,
}

fn fetch() -> Fetch {
    let offline = std::env::var("DUCKLE_OFFLINE")
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);
    if crate::policy::duckdb_external_io_denied() {
        Fetch::PolicyDenied
    } else if offline {
        Fetch::Offline
    } else {
        Fetch::Allowed
    }
}

/// The SQL the check runs. Only `LOAD` where downloads are not allowed, so a
/// backend that is not installed yet falls back rather than being fetched.
fn probe_sql(fetch: Fetch) -> String {
    let load = match fetch {
        // Sirius's own instructions: httpfs, then a timeout long enough for a
        // backend of 0.7 to 1.2 GB.
        Fetch::Allowed => {
            "INSTALL httpfs; LOAD httpfs; SET http_timeout = 900; INSTALL sirius; LOAD sirius;\n"
                .to_string()
        }
        Fetch::Offline => "SET autoinstall_known_extensions=false;\nLOAD sirius;\n".to_string(),
        Fetch::PolicyDenied => format!("{}LOAD sirius;\n", crate::policy::DUCKDB_OFF_NETWORK),
    };
    // Status first: a trial query that Sirius refuses stops the script under
    // -bail, and the status has to be out by then. The trial turns fallback off
    // so a query Sirius declines says why instead of quietly running on the CPU.
    format!(
        "{QUIET}{load}SELECT * FROM sirius_status();\n\
         CREATE TEMP TABLE duckle_gpu_probe AS SELECT range AS k FROM range(100000);\n\
         SET enable_duckdb_fallback = false;\n\
         SELECT sum(k) AS s FROM duckle_gpu_probe WHERE k % 3 = 0;\n"
    )
}

/// Sirius logs through DuckDB, and the CLI prints its warnings on stdout, in
/// the middle of the `-json` output Duckle reads ("disk memory space is not
/// configured", "no GPU scan operators found in query", measured on 2.0.1).
/// Errors still reach stderr at this level. `CALL disable_logging()` quiets it
/// too, but prints an empty result of its own.
const QUIET: &str = "SET logging_level = 'error';\n";

/// `text` without the CLI's colour codes.
fn plain(text: &str) -> String {
    let mut out = String::new();
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            // ESC [ ... letter
            if chars.peek() == Some(&'[') {
                chars.next();
                for d in chars.by_ref() {
                    if d.is_ascii_alphabetic() {
                        break;
                    }
                }
            }
            continue;
        }
        out.push(c);
    }
    out
}

/// The first JSON array of rows in `out`, which `-json` prints for the first
/// result. Found by `[{` rather than `[`, so a log line that got through, which
/// opens with `[source.cpp:123]`, is not taken for it.
fn first_json_array(out: &str) -> Option<serde_json::Value> {
    let out = plain(out);
    let start = out.find("[{")?;
    serde_json::Deserializer::from_str(&out[start..])
        .into_iter::<serde_json::Value>()
        .next()?
        .ok()
}

/// The reason in a DuckDB error, without the CLI's colour codes or blank lines.
fn reason_from(stderr: &str) -> String {
    let plain = plain(stderr);
    let lines: Vec<&str> = plain
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .collect();
    // A refusal names itself on one line; anything else, keep the first few.
    lines
        .iter()
        .find(|l| l.contains("Error"))
        .map(|l| l.to_string())
        .unwrap_or_else(|| lines.iter().take(3).cloned().collect::<Vec<_>>().join(" "))
}

/// Read what one run of the check printed.
fn interpret(stdout: &str, stderr: &str, exit_ok: bool) -> Probe {
    let status = first_json_array(stdout)
        .and_then(|v| v.get(0).cloned())
        .and_then(|row| serde_json::from_value::<SiriusStatus>(row).ok());
    let Some(status) = status else {
        let why = reason_from(stderr);
        return Probe::Unavailable(if why.is_empty() {
            "Sirius did not load and DuckDB gave no reason".into()
        } else {
            why
        });
    };
    if !status.backend_loaded || !status.gpu_execution {
        return Probe::Unavailable(status.fallback_reason.unwrap_or_else(|| {
            if status.driver_found {
                "Sirius loaded without a GPU backend".into()
            } else {
                "Sirius found no NVIDIA driver".into()
            }
        }));
    }
    Probe::Ready {
        backend: status.backend.unwrap_or_default(),
        cuda: status.cuda_driver_version.unwrap_or_default(),
        caveat: (!exit_ok)
            .then(|| reason_from(stderr))
            .filter(|r| !r.is_empty()),
    }
}

/// Load Sirius once in a throwaway DuckDB and say what happened.
fn probe(bin: &Path) -> Probe {
    let mut cmd = std::process::Command::new(bin);
    cmd.arg(":memory:")
        .arg("-no-init")
        .arg("-json")
        .arg("-bail");
    cmd.env("DUCKDB_AGENT_MODE", "0");
    if let Some(cfg) = managed_config() {
        cmd.env("SIRIUS_CONFIG_FILE", cfg);
    }
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => {
            return Probe::Unavailable(format!("could not start duckdb to check the GPU: {e}"))
        }
    };
    if let Some(mut stdin) = child.stdin.take() {
        let _ = stdin.write_all(probe_sql(fetch()).as_bytes());
    }
    match child.wait_with_output() {
        Ok(out) => interpret(
            &String::from_utf8_lossy(&out.stdout),
            &String::from_utf8_lossy(&out.stderr),
            out.status.success(),
        ),
        Err(e) => Probe::Unavailable(format!("the GPU check did not finish: {e}")),
    }
}

/// Checks already made in this process, by DuckDB binary.
fn checks() -> &'static Mutex<HashMap<PathBuf, Probe>> {
    static CHECKS: OnceLock<Mutex<HashMap<PathBuf, Probe>>> = OnceLock::new();
    CHECKS.get_or_init(Default::default)
}

/// Whether Sirius loads with `bin` on this machine, checking once per process.
///
/// The lock is held through the check, so two runs that start together do not
/// both download the backend: DuckDB's concurrent INSTALLs race each other.
fn checked(bin: &Path) -> Probe {
    let mut known = checks().lock().unwrap_or_else(|e| e.into_inner());
    if let Some(p) = known.get(bin) {
        return p.clone();
    }
    let p = probe(bin);
    known.insert(bin.to_path_buf(), p.clone());
    p
}

/// Forget earlier checks, so the next run or status looks again.
pub fn forget() {
    checks().lock().unwrap_or_else(|e| e.into_inner()).clear();
}

/// The machine-wide lock that keeps Sirius to one DuckDB process at a time.
fn gpu_lock_path() -> PathBuf {
    std::env::temp_dir().join("duckle-locks").join("gpu.lock")
}

/// A run that has the GPU. Holding it holds the machine's GPU lock.
#[derive(Debug)]
pub struct GpuRun {
    _lock: crate::runlock::RunLock,
    /// `on`: the run asked for the GPU, so DuckDB is told to refuse to run
    /// without it rather than fall back.
    pub strict: bool,
}

/// What a run does about the GPU, and what its log says.
#[derive(Debug)]
pub enum RunGpu {
    /// Load Sirius in this run's DuckDB processes.
    Gpu(GpuRun, String),
    /// Leave it out. The note is what the run log says, if anything.
    Cpu(Option<String>),
}

/// The pure part of the decision, so every branch can be tested anywhere.
fn decide(
    mode: GpuMode,
    platform: Option<String>,
    driver: bool,
    probe: impl FnOnce() -> Probe,
    lock: impl FnOnce() -> crate::runlock::AcquireOutcome,
) -> Result<RunGpu, String> {
    let strict = mode == GpuMode::On;
    // `on` fails with the reason; `auto` stays quiet where no GPU was ever
    // possible, and says why where one looked possible.
    let refuse = |why: String, say_in_auto: bool| -> Result<RunGpu, String> {
        if strict {
            Err(format!(
                "GPU acceleration is On, but this run cannot use the GPU: {why}"
            ))
        } else {
            Ok(RunGpu::Cpu(
                say_in_auto.then(|| format!("GPU: not used: {why}")),
            ))
        }
    };
    if mode == GpuMode::Off {
        return Ok(RunGpu::Cpu(None));
    }
    if let Some(why) = platform {
        return refuse(why, false);
    }
    if !driver {
        return refuse("no NVIDIA driver was found (libcuda.so.1)".into(), false);
    }
    let (backend, cuda, caveat) = match probe() {
        Probe::Ready {
            backend,
            cuda,
            caveat,
        } => (backend, cuda, caveat),
        Probe::Unavailable(why) => return refuse(format!("Sirius could not start: {why}"), true),
    };
    let lock = match lock() {
        crate::runlock::AcquireOutcome::Claimed(l) => l,
        crate::runlock::AcquireOutcome::HeldByOther => {
            return refuse(
                "another run has the GPU, and Sirius takes it whole".into(),
                true,
            )
        }
        crate::runlock::AcquireOutcome::Unusable(e) => {
            return refuse(
                format!(
                    "the GPU lock at {} cannot be taken: {e}",
                    gpu_lock_path().display()
                ),
                true,
            )
        }
    };
    let mut note =
        format!("GPU: NVIDIA Sirius ({backend}, CUDA {cuda}) runs the queries it supports");
    if let Some(c) = caveat {
        note.push_str(&format!("; a trial query still ran on the CPU: {c}"));
    }
    Ok(RunGpu::Gpu(
        GpuRun {
            _lock: lock,
            strict,
        },
        note,
    ))
}

/// Decide the GPU for one run with `bin`. `Err` only under `on`.
pub fn for_run(bin: &Path) -> Result<RunGpu, String> {
    let mode = mode()?;
    decide(
        mode,
        platform_reason(),
        // Only looked for where Sirius could exist at all.
        platform_reason().is_none() && driver_found(),
        || checked(bin),
        || crate::runlock::try_acquire_path(gpu_lock_path(), "gpu"),
    )
}

/// The statements that put Sirius into a DuckDB session for a run.
pub(crate) fn session_sql(run: &GpuRun) -> String {
    // Quiet first, or Sirius's warnings land in the run's JSON output.
    if run.strict {
        // Set after the load: set before it, a load that falls back fails too,
        // but the error then names the setting rather than the cause.
        format!("{QUIET}LOAD sirius;\nSET gpu_execution = true;\n")
    } else {
        format!("{QUIET}LOAD sirius;\n")
    }
}

/// Where the GPU stands, for Settings and `duckle-runner gpu`.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GpuStatus {
    pub mode: String,
    /// `ready`, `unavailable` or `off`.
    pub state: String,
    pub reason: Option<String>,
    pub backend: Option<String>,
    pub cuda_version: Option<String>,
}

/// The status a run would find, checking (and downloading the backend) if this
/// process has not yet. `recheck` forgets the earlier answer first.
pub fn status(bin: &Path, recheck: bool) -> GpuStatus {
    if recheck {
        forget();
    }
    let unavailable = |mode: GpuMode, why: String| GpuStatus {
        mode: mode.as_str().into(),
        state: "unavailable".into(),
        reason: Some(why),
        backend: None,
        cuda_version: None,
    };
    let mode = match mode() {
        Ok(m) => m,
        Err(e) => return unavailable(GpuMode::Auto, e),
    };
    if mode == GpuMode::Off {
        return GpuStatus {
            mode: mode.as_str().into(),
            state: "off".into(),
            reason: None,
            backend: None,
            cuda_version: None,
        };
    }
    if let Some(why) = platform_reason() {
        return unavailable(mode, why);
    }
    if !driver_found() {
        return unavailable(mode, "no NVIDIA driver was found (libcuda.so.1)".into());
    }
    match checked(bin) {
        Probe::Ready {
            backend,
            cuda,
            caveat,
        } => GpuStatus {
            mode: mode.as_str().into(),
            state: "ready".into(),
            reason: caveat.map(|c| format!("a trial query still ran on the CPU: {c}")),
            backend: Some(backend),
            cuda_version: Some(cuda),
        },
        Probe::Unavailable(why) => unavailable(mode, format!("Sirius could not start: {why}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runlock::AcquireOutcome;

    #[test]
    fn the_mode_reads_as_typed_and_defaults_to_auto() {
        assert_eq!(parse_mode(""), Ok(GpuMode::Auto));
        assert_eq!(parse_mode(" Auto "), Ok(GpuMode::Auto));
        assert_eq!(parse_mode("ON"), Ok(GpuMode::On));
        assert_eq!(parse_mode("off"), Ok(GpuMode::Off));
        let err = parse_mode("yes").unwrap_err();
        assert!(
            err.contains("auto, on or off") && err.contains("'yes'"),
            "{err}"
        );
    }

    #[test]
    fn a_driver_is_found_by_any_of_the_places_sirius_loads_it_from() {
        let tmp = tempfile::tempdir().unwrap();
        let none = tmp.path().join("absent");
        let empty_cache = tmp.path().join("ld.so.cache");
        std::fs::write(&empty_cache, b"libc.so.6\0libm.so.6\0").unwrap();
        assert!(!driver_found_in(&none, &none, &empty_cache, ""));

        let proc_version = tmp.path().join("version");
        std::fs::write(&proc_version, "NVRM version").unwrap();
        assert!(
            driver_found_in(&proc_version, &none, &empty_cache, ""),
            "native driver"
        );

        let wsl = tmp.path().join("wsl");
        std::fs::create_dir_all(&wsl).unwrap();
        std::fs::write(wsl.join("libcuda.so.1"), b"").unwrap();
        assert!(driver_found_in(&none, &wsl, &empty_cache, ""), "WSL2");

        let cache = tmp.path().join("ld.so.cache.gpu");
        std::fs::write(
            &cache,
            b"\0libcuda.so.1\0/usr/lib/x86_64-linux-gnu/libcuda.so.1\0",
        )
        .unwrap();
        assert!(
            driver_found_in(&none, &none, &cache, ""),
            "container loader cache"
        );

        let lib = std::env::join_paths([none.clone(), wsl.clone()]).unwrap();
        assert!(
            driver_found_in(&none, &none, &empty_cache, &lib.to_string_lossy()),
            "LD_LIBRARY_PATH"
        );
    }

    #[test]
    fn the_host_budget_is_a_quarter_of_ram_and_never_under_a_gib() {
        const GIB: u64 = 1024 * 1024 * 1024;
        assert_eq!(
            host_budget("MemTotal:       16384000 kB\nMemFree: 1 kB\n"),
            16384000 * 1024 / 4
        );
        assert_eq!(
            host_budget("MemTotal: 2048000 kB\n"),
            GIB,
            "a small machine still gets 1 GiB"
        );
        assert_eq!(host_budget(""), GIB, "no /proc/meminfo");
        let text = config_text(4 * GIB);
        assert!(text.contains("capacity_bytes: 4294967296"), "{text}");
        // An absolute budget and an explicit pool count: the two settings whose
        // NUMA-sized defaults stop Sirius starting where there is no NUMA node.
        assert!(
            text.contains("initial_number_pools: 1") && !text.contains("capacity_fraction"),
            "{text}"
        );
        assert!(text.contains("usage_limit_fraction: 0.8"), "{text}");
    }

    #[test]
    fn a_config_the_user_wrote_wins() {
        let tmp = tempfile::tempdir().unwrap();
        let home = tmp.path().join("home");
        let cwd = tmp.path().join("cwd");
        std::fs::create_dir_all(home.join(".sirius")).unwrap();
        std::fs::create_dir_all(&cwd).unwrap();
        // The process may have SIRIUS_CONFIG_FILE from outside; this test is
        // about the two files, so it only asserts when the variable is unset.
        if std::env::var_os("SIRIUS_CONFIG_FILE").is_none() {
            assert!(!user_config_present(Some(&home), Some(&cwd)));
        }
        std::fs::write(cwd.join("sirius.yaml"), "sirius: {}\n").unwrap();
        assert!(
            user_config_present(Some(&home), Some(&cwd)),
            "./sirius.yaml"
        );
        std::fs::remove_file(cwd.join("sirius.yaml")).unwrap();
        std::fs::write(home.join(".sirius").join("sirius.yaml"), "sirius: {}\n").unwrap();
        assert!(
            user_config_present(Some(&home), Some(&cwd)),
            "~/.sirius/sirius.yaml"
        );
    }

    #[test]
    fn the_check_downloads_only_where_downloads_are_allowed() {
        let open = probe_sql(Fetch::Allowed);
        assert!(
            open.contains("INSTALL sirius;") && open.contains("SET http_timeout = 900;"),
            "{open}"
        );
        let offline = probe_sql(Fetch::Offline);
        assert!(!offline.contains("INSTALL"), "{offline}");
        assert!(
            offline.contains("SET autoinstall_known_extensions=false;"),
            "{offline}"
        );
        // Offline only stops downloads: an installed backend, which DuckDB
        // keeps as a community extension, must still load.
        assert!(!offline.contains("allow_community_extensions"), "{offline}");
        let policy = probe_sql(Fetch::PolicyDenied);
        assert!(
            !policy.contains("INSTALL") && policy.contains(crate::policy::DUCKDB_OFF_NETWORK),
            "{policy}"
        );
        for sql in [&open, &offline, &policy] {
            // The status is read before the trial, which -bail stops at if refused.
            let status = sql.find("sirius_status()").unwrap();
            assert!(
                status < sql.find("enable_duckdb_fallback").unwrap(),
                "{sql}"
            );
            // Quiet before anything loads, or the status arrives inside a warning.
            assert!(sql.starts_with(QUIET), "{sql}");
        }
    }

    /// What 2.0.1 printed on stdout with Sirius loaded and logging left as it
    /// is: the warning opens with `[`, ahead of the status. Quiet keeps it off
    /// stdout; this is the parser not tripping over it if one gets through.
    #[test]
    fn a_warning_ahead_of_the_status_is_not_taken_for_it() {
        let noisy = format!(
            "\u{1b}[90mWARNING:\n\u{1b}[00m\u{1b}[90m[sirius_context.cpp:1002] SiriusContext: disk memory space is not configured; spilling GPU or HOST data to disk is disabled.\n\n\u{1b}[00m{READY}\n"
        );
        assert!(matches!(interpret(&noisy, "", true), Probe::Ready { .. }));
    }

    // What DuckDB 2.0.1's CLI printed in each case, on the RTX 4050 test box.
    const READY: &str = r#"[{"driver_found":true,"cuda_driver_version":"13.1","backend":"sirius_cuda13","backend_loaded":true,"gpu_execution":true,"fallback_reason":null}]"#;

    #[test]
    fn a_check_reads_ready_unavailable_and_a_trial_that_stayed_on_the_cpu() {
        assert_eq!(
            interpret(&format!("{READY}\n[{{\"s\":1}}]\n"), "", true),
            Probe::Ready {
                backend: "sirius_cuda13".into(),
                cuda: "13.1".into(),
                caveat: None
            }
        );
        let declined = "\u{1b}[31mNot implemented Error: GPU plan generation failed: Table function 'seq_scan' is not supported in Sirius (unverified callbacks, overload or bind data)\u{1b}[0m\n";
        match interpret(READY, declined, false) {
            Probe::Ready {
                caveat: Some(c), ..
            } => {
                assert!(
                    c.starts_with("Not implemented Error: GPU plan generation failed"),
                    "{c}"
                )
            }
            other => panic!("{other:?}"),
        }
        // A container started without the GPU. The trial then fails too (the
        // setting it turns off comes with the backend), which changes nothing.
        let no_driver = r#"[{"driver_found":false,"cuda_driver_version":null,"backend":null,"backend_loaded":false,"gpu_execution":false,"fallback_reason":"Sirius requires an NVIDIA driver supporting CUDA 12.8 or newer: cannot open libcuda.so.1 (libcuda.so.1: cannot open shared object file: No such file or directory)"}]"#;
        let unknown_setting =
            "Catalog Error: unrecognized configuration parameter \"enable_duckdb_fallback\"\n";
        match interpret(no_driver, unknown_setting, false) {
            Probe::Unavailable(why) => {
                assert!(why.starts_with("Sirius requires an NVIDIA driver"), "{why}")
            }
            other => panic!("{other:?}"),
        }
        // A load that fails outright prints no status at all.
        let numa = "Invalid Input Error: Initialization function \"sirius_duckdb_cpp_init\" threw an exception: \"NUMA region capacity fraction requested but the memory capacity of NUMA node -1 is unknown\"\n";
        match interpret("", numa, false) {
            Probe::Unavailable(why) => assert!(why.contains("NUMA node -1"), "{why}"),
            other => panic!("{other:?}"),
        }
    }

    /// A lock nobody else holds. Its folder outlives the call, so the lock is
    /// never on a file whose directory has gone.
    fn claimed() -> AcquireOutcome {
        use std::sync::atomic::{AtomicU32, Ordering};
        static N: AtomicU32 = AtomicU32::new(0);
        let dir = std::env::temp_dir().join(format!(
            "duckle-gpu-test-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        crate::runlock::try_acquire_path(dir.join("gpu.lock"), "gpu")
    }
    fn ready() -> Probe {
        Probe::Ready {
            backend: "sirius_cuda13".into(),
            cuda: "13.1".into(),
            caveat: None,
        }
    }
    fn unreachable_probe() -> Probe {
        panic!("the check must not run here")
    }
    fn unreachable_lock() -> AcquireOutcome {
        panic!("the lock must not be taken here")
    }

    #[test]
    fn auto_uses_the_gpu_where_sirius_starts_and_stays_quiet_where_it_never_could() {
        match decide(GpuMode::Auto, None, true, ready, claimed).unwrap() {
            RunGpu::Gpu(run, note) => {
                assert!(!run.strict);
                assert!(
                    note.contains("sirius_cuda13") && note.contains("CUDA 13.1"),
                    "{note}"
                );
                assert_eq!(session_sql(&run), format!("{QUIET}LOAD sirius;\n"));
            }
            other => panic!("{other:?}"),
        }
        // Windows or macOS, and Linux with no driver: nothing to say.
        let windows = Some("DuckDB builds Sirius for Linux on x86-64 and arm64 only".to_string());
        assert!(matches!(
            decide(
                GpuMode::Auto,
                windows,
                false,
                unreachable_probe,
                unreachable_lock
            )
            .unwrap(),
            RunGpu::Cpu(None)
        ));
        assert!(matches!(
            decide(
                GpuMode::Auto,
                None,
                false,
                unreachable_probe,
                unreachable_lock
            )
            .unwrap(),
            RunGpu::Cpu(None)
        ));
        // A driver, but Sirius will not start: the run goes on, and says why.
        match decide(
            GpuMode::Auto,
            None,
            true,
            || Probe::Unavailable("NUMA node -1".into()),
            unreachable_lock,
        )
        .unwrap()
        {
            RunGpu::Cpu(Some(note)) => assert!(note.contains("NUMA node -1"), "{note}"),
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            decide(
                GpuMode::Off,
                None,
                true,
                unreachable_probe,
                unreachable_lock
            )
            .unwrap(),
            RunGpu::Cpu(None)
        ));
    }

    #[test]
    fn on_fails_a_run_that_cannot_have_the_gpu_and_says_why() {
        let err = decide(
            GpuMode::On,
            Some("this is windows on x86_64".into()),
            false,
            unreachable_probe,
            unreachable_lock,
        )
        .unwrap_err();
        assert!(
            err.contains("GPU acceleration is On") && err.contains("windows"),
            "{err}"
        );
        let err = decide(
            GpuMode::On,
            None,
            true,
            || Probe::Unavailable("NUMA node -1".into()),
            unreachable_lock,
        )
        .unwrap_err();
        assert!(err.contains("NUMA node -1"), "{err}");
        match decide(GpuMode::On, None, true, ready, claimed).unwrap() {
            RunGpu::Gpu(run, _) => {
                assert!(run.strict);
                assert_eq!(
                    session_sql(&run),
                    format!("{QUIET}LOAD sirius;\nSET gpu_execution = true;\n")
                );
            }
            other => panic!("{other:?}"),
        }
    }

    /// Every DuckDB process a run with the GPU starts loads Sirius: the session
    /// lines ride the one builder both execution paths use, after every setting.
    #[test]
    fn a_run_with_the_gpu_loads_sirius_in_every_process_it_starts() {
        let run = match decide(GpuMode::Auto, None, true, ready, claimed).unwrap() {
            RunGpu::Gpu(run, _) => run,
            other => panic!("{other:?}"),
        };
        let with = crate::resource_pragmas(None, None, Some(&run));
        assert!(with.ends_with(&session_sql(&run)), "{with}");
        let without = crate::resource_pragmas(None, None, None);
        assert!(
            !without.contains("sirius") && !without.contains("logging_level"),
            "{without}"
        );
    }

    #[test]
    fn a_second_run_does_not_get_the_gpu_while_the_first_holds_it() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("gpu.lock");
        let first = match decide(GpuMode::Auto, None, true, ready, || {
            crate::runlock::try_acquire_path(path.clone(), "gpu")
        })
        .unwrap()
        {
            RunGpu::Gpu(run, _) => run,
            other => panic!("{other:?}"),
        };
        match decide(GpuMode::Auto, None, true, ready, || {
            crate::runlock::try_acquire_path(path.clone(), "gpu")
        })
        .unwrap()
        {
            RunGpu::Cpu(Some(note)) => assert!(note.contains("another run has the GPU"), "{note}"),
            other => panic!("{other:?}"),
        }
        let err = decide(GpuMode::On, None, true, ready, || {
            crate::runlock::try_acquire_path(path.clone(), "gpu")
        })
        .unwrap_err();
        assert!(err.contains("another run has the GPU"), "{err}");
        drop(first);
        assert!(matches!(
            decide(GpuMode::Auto, None, true, ready, || {
                crate::runlock::try_acquire_path(path.clone(), "gpu")
            })
            .unwrap(),
            RunGpu::Gpu(..)
        ));
    }
}
