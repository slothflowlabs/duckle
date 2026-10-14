//! First-launch provisioning of a free dbt engine for the xf.dbt node, with zero
//! setup required from the user.
//!
//! PRIMARY: dbt Fusion (Rust, no Python). ~50x faster startup than dbt-core, so
//! a dbt node runs in well under a second instead of paying a ~2.3s Python import
//! tax per invocation. The Fusion CLI is free to use (no account, no payment); we
//! fetch the native binary from dbt's official public CDN at first launch - the
//! same fetch-not-bundle model we use for DuckDB and uv, so nothing proprietary
//! is redistributed inside Duckle.
//!
//! FALLBACK: the Apache-2.0 dbt-core line via uv (`uv tool install dbt-core
//! --with dbt-duckdb`). Used only when Fusion can't be fetched (offline, or an
//! unsupported OS/arch). uv brings its own standalone Python.
//!
//! Whichever engine is provisioned, its `dbt` path is published as DUCKLE_DBT_BIN
//! for the engine's resolve_dbt_bin(). Everything lives under the app-data dir
//! (`<app_data>/dbt-fusion/` or `<app_data>/dbt/`), isolated from any system dbt.

use std::path::{Path, PathBuf};

/// Windows: suppress the console window that pops up when this GUI process
/// spawns a console subprocess (uv, dbt). No-op on other platforms.
fn no_window(cmd: &mut std::process::Command) {
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
    }
    let _ = cmd;
}

/// Directory holding the provisioned dbt toolchain.
fn dbt_root(app_data: &Path) -> PathBuf {
    app_data.join("dbt")
}

/// Path to the provisioned `dbt` executable (uv installs the entrypoint here).
pub fn dbt_path(app_data: &Path) -> PathBuf {
    let exe = if cfg!(windows) { "dbt.exe" } else { "dbt" };
    dbt_root(app_data).join("bin").join(exe)
}

/// Directory holding the provisioned dbt Fusion binary.
fn fusion_root(app_data: &Path) -> PathBuf {
    app_data.join("dbt-fusion")
}

/// Path to the provisioned Fusion `dbt` executable.
pub fn fusion_path(app_data: &Path) -> PathBuf {
    let exe = if cfg!(windows) { "dbt.exe" } else { "dbt" };
    fusion_root(app_data).join(exe)
}

/// True when the preferred engine (Fusion) is already provisioned.
pub fn fusion_present(app_data: &Path) -> bool {
    fusion_path(app_data).exists()
}

/// True when any dbt engine (Fusion or the dbt-core fallback) is available.
pub fn is_installed(app_data: &Path) -> bool {
    fusion_present(app_data) || dbt_path(app_data).exists()
}

/// Publish whichever provisioned dbt the engine should use as DUCKLE_DBT_BIN,
/// preferring Fusion. Cheap, no network - safe to call on every startup.
pub fn publish_if_present(app_data: &Path) {
    let f = fusion_path(app_data);
    if f.exists() {
        std::env::set_var("DUCKLE_DBT_BIN", &f);
        return;
    }
    let c = dbt_path(app_data);
    if c.exists() {
        std::env::set_var("DUCKLE_DBT_BIN", &c);
    }
}

/// uv release asset name for this OS/arch (astral-sh/uv).
fn uv_asset() -> Option<&'static str> {
    Some(match (std::env::consts::OS, std::env::consts::ARCH) {
        ("windows", "x86_64") => "uv-x86_64-pc-windows-msvc.zip",
        ("windows", "aarch64") => "uv-aarch64-pc-windows-msvc.zip",
        ("linux", "x86_64") => "uv-x86_64-unknown-linux-gnu.tar.gz",
        ("linux", "aarch64") => "uv-aarch64-unknown-linux-gnu.tar.gz",
        ("macos", "x86_64") => "uv-x86_64-apple-darwin.tar.gz",
        ("macos", "aarch64") => "uv-aarch64-apple-darwin.tar.gz",
        _ => return None,
    })
}

/// Resolve uv: prefer a system `uv` on PATH; otherwise download a pinned-latest
/// uv release into `<app_data>/dbt/uv/` and return that path.
pub(crate) fn ensure_uv(app_data: &Path) -> Result<PathBuf, String> {
    // 1. System uv on PATH.
    let on_path = if cfg!(windows) { "uv.exe" } else { "uv" };
    let mut probe = std::process::Command::new(on_path);
    no_window(&mut probe);
    if probe
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
    {
        return Ok(PathBuf::from(on_path));
    }

    // 2. Previously downloaded uv.
    let uv_dir = dbt_root(app_data).join("uv");
    let uv_exe = uv_dir.join(if cfg!(windows) { "uv.exe" } else { "uv" });
    if uv_exe.exists() {
        return Ok(uv_exe);
    }

    // 3. Download a fresh uv from astral-sh/uv (latest).
    let asset = uv_asset().ok_or_else(|| {
        format!(
            "no uv build for this platform ({}/{})",
            std::env::consts::OS,
            std::env::consts::ARCH
        )
    })?;
    let url = format!(
        "https://github.com/astral-sh/uv/releases/latest/download/{}",
        asset
    );
    std::fs::create_dir_all(&uv_dir).map_err(|e| e.to_string())?;
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(300))
        .build()
        .map_err(|e| e.to_string())?;
    let bytes = client
        .get(&url)
        .send()
        .and_then(|r| r.error_for_status())
        .and_then(|r| r.bytes())
        .map_err(|e| format!("download uv: {e}"))?;
    // Before extraction, because the archive yields an executable.
    crate::engine_manager::verify_download(
        "the uv download",
        &crate::engine_manager::hex_sha256(&bytes),
        crate::engine_manager::UV_SHA256,
    )?;

    // uv archives contain the binary under `uv-<target>/uv[.exe]`; extract just it.
    if asset.ends_with(".zip") {
        let mut zip = zip::ZipArchive::new(std::io::Cursor::new(&bytes))
            .map_err(|e| format!("open uv zip: {e}"))?;
        for i in 0..zip.len() {
            let mut f = zip.by_index(i).map_err(|e| e.to_string())?;
            let name = f.name().map_err(|e| e.to_string())?.rsplit('/').next().unwrap_or("").to_string();
            if name == "uv.exe" || name == "uv" {
                let mut out = std::fs::File::create(&uv_exe).map_err(|e| e.to_string())?;
                std::io::copy(&mut f, &mut out).map_err(|e| e.to_string())?;
                break;
            }
        }
    } else {
        let gz = flate2::read::GzDecoder::new(std::io::Cursor::new(&bytes));
        let mut archive = tar::Archive::new(gz);
        for entry in archive.entries().map_err(|e| e.to_string())? {
            let mut entry = entry.map_err(|e| e.to_string())?;
            let path = entry.path().map_err(|e| e.to_string())?.into_owned();
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_string();
            if name == "uv" {
                let mut out = std::fs::File::create(&uv_exe).map_err(|e| e.to_string())?;
                std::io::copy(&mut entry, &mut out).map_err(|e| e.to_string())?;
                break;
            }
        }
    }
    if !uv_exe.exists() {
        return Err("uv binary not found in the downloaded archive".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&uv_exe, std::fs::Permissions::from_mode(0o755));
    }
    Ok(uv_exe)
}

/// Fallback engine: ensure the Apache dbt-core line is provisioned (idempotent)
/// and return its path. Downloads uv if needed, then `uv tool install dbt-core
/// --with dbt-duckdb` into an isolated app-data dir (uv fetches its own Python).
/// Network + minutes on first call.
fn ensure_dbt_core(app_data: &Path) -> Result<PathBuf, String> {
    let dbt = dbt_path(app_data);
    if dbt.exists() {
        return Ok(dbt);
    }
    let uv = ensure_uv(app_data)?;
    let root = dbt_root(app_data);
    let tool_dir = root.join("tools");
    let bin_dir = root.join("bin");
    std::fs::create_dir_all(&bin_dir).map_err(|e| e.to_string())?;

    // dbt-duckdb ships no entrypoint; the `dbt` CLI comes from dbt-core, so
    // install dbt-core WITH the duckdb adapter as a dependency.
    let mut cmd = std::process::Command::new(&uv);
    no_window(&mut cmd);
    let output = cmd
        .args(["tool", "install", "dbt-core", "--with", "dbt-duckdb"])
        .env("UV_TOOL_DIR", &tool_dir)
        .env("UV_TOOL_BIN_DIR", &bin_dir)
        .output()
        .map_err(|e| format!("run uv: {e}"))?;
    if !dbt.exists() {
        let err = String::from_utf8_lossy(&output.stderr);
        return Err(format!(
            "uv tool install dbt-core failed: {}",
            err.trim().chars().rev().take(600).collect::<String>().chars().rev().collect::<String>()
        ));
    }
    Ok(dbt)
}

/// dbt Fusion release target triple + archive extension for this OS/arch. None
/// on platforms dbt Labs does not publish a Fusion build for.
fn fusion_target() -> Option<(&'static str, &'static str)> {
    Some(match (std::env::consts::OS, std::env::consts::ARCH) {
        ("windows", "x86_64") => ("x86_64-pc-windows-msvc", "zip"),
        ("windows", "aarch64") => ("aarch64-pc-windows-msvc", "zip"),
        ("linux", "x86_64") => ("x86_64-unknown-linux-gnu", "tar.gz"),
        ("linux", "aarch64") => ("aarch64-unknown-linux-gnu", "tar.gz"),
        ("macos", "x86_64") => ("x86_64-apple-darwin", "tar.gz"),
        ("macos", "aarch64") => ("aarch64-apple-darwin", "tar.gz"),
        _ => return None,
    })
}

/// Resolve the current Fusion release tag from dbt's public CDN manifest,
/// falling back to a known-good pin when the manifest is unreachable.
fn fusion_version(client: &reqwest::blocking::Client) -> String {
    const PINNED: &str = "v2.0.0-preview.189";
    client
        .get("https://public.cdn.getdbt.com/fs/versions.json")
        .send()
        .and_then(|r| r.error_for_status())
        .and_then(|r| r.text())
        .ok()
        .and_then(|t| serde_json::from_str::<serde_json::Value>(&t).ok())
        .and_then(|j| {
            j.get("latest")
                .and_then(|l| l.get("tag"))
                .and_then(|t| t.as_str())
                .map(String::from)
        })
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| PINNED.to_string())
}

/// Fetch the free dbt Fusion CLI (native Rust, no Python) from dbt's official
/// public CDN into `<app_data>/dbt-fusion/`. Idempotent. ~94 MB download on the
/// first call; nothing proprietary is bundled inside Duckle.
fn ensure_fusion(app_data: &Path) -> Result<PathBuf, String> {
    let dbt = fusion_path(app_data);
    if dbt.exists() {
        return Ok(dbt);
    }
    let (target, ext) = fusion_target().ok_or_else(|| {
        format!(
            "no dbt Fusion build for this platform ({}/{})",
            std::env::consts::OS,
            std::env::consts::ARCH
        )
    })?;
    let root = fusion_root(app_data);
    std::fs::create_dir_all(&root).map_err(|e| e.to_string())?;
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(600))
        .build()
        .map_err(|e| e.to_string())?;
    let version = fusion_version(&client);
    let url = format!("https://public.cdn.getdbt.com/fs/cli/fs-{version}-{target}.{ext}");
    let bytes = client
        .get(&url)
        .send()
        .and_then(|r| r.error_for_status())
        .and_then(|r| r.bytes())
        .map_err(|e| format!("download dbt Fusion ({url}): {e}"))?;
    // Before extraction, because the archive yields an executable.
    crate::engine_manager::verify_download(
        "the dbt Fusion download",
        &crate::engine_manager::hex_sha256(&bytes),
        crate::engine_manager::FUSION_SHA256,
    )?;

    // The archive carries a single `dbt[.exe]`; extract just it.
    let exe_name = if cfg!(windows) { "dbt.exe" } else { "dbt" };
    if ext == "zip" {
        let mut zip = zip::ZipArchive::new(std::io::Cursor::new(&bytes))
            .map_err(|e| format!("open Fusion zip: {e}"))?;
        for i in 0..zip.len() {
            let mut f = zip.by_index(i).map_err(|e| e.to_string())?;
            let name = f.name().map_err(|e| e.to_string())?.rsplit('/').next().unwrap_or("").to_string();
            if name == exe_name {
                let mut out = std::fs::File::create(&dbt).map_err(|e| e.to_string())?;
                std::io::copy(&mut f, &mut out).map_err(|e| e.to_string())?;
                break;
            }
        }
    } else {
        let gz = flate2::read::GzDecoder::new(std::io::Cursor::new(&bytes));
        let mut archive = tar::Archive::new(gz);
        for entry in archive.entries().map_err(|e| e.to_string())? {
            let mut entry = entry.map_err(|e| e.to_string())?;
            let path = entry.path().map_err(|e| e.to_string())?.into_owned();
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_string();
            if name == exe_name {
                let mut out = std::fs::File::create(&dbt).map_err(|e| e.to_string())?;
                std::io::copy(&mut entry, &mut out).map_err(|e| e.to_string())?;
                break;
            }
        }
    }
    if !dbt.exists() {
        return Err("dbt binary not found in the Fusion archive".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&dbt, std::fs::Permissions::from_mode(0o755));
    }
    Ok(dbt)
}

/// Ensure a dbt engine is provisioned (idempotent) and publish it as
/// DUCKLE_DBT_BIN. Prefers Fusion (Rust, ~50x faster startup); falls back to the
/// Apache dbt-core line via uv if Fusion cannot be fetched (offline / unsupported
/// arch). Network + time on the first call.
pub fn ensure(app_data: &Path) -> Result<PathBuf, String> {
    match ensure_fusion(app_data) {
        Ok(p) => {
            std::env::set_var("DUCKLE_DBT_BIN", &p);
            Ok(p)
        }
        Err(fe) => match ensure_dbt_core(app_data) {
            Ok(p) => {
                std::env::set_var("DUCKLE_DBT_BIN", &p);
                Ok(p)
            }
            Err(ce) => Err(format!(
                "dbt Fusion unavailable ({fe}); dbt-core fallback failed ({ce})"
            )),
        },
    }
}
