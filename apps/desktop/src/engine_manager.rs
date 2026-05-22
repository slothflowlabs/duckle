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

pub const DUCKDB_VERSION: &str = "1.1.3";
pub const SLOTHDB_VERSION: &str = "0.1.0";

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
    description: "Default engine — local analytics, file formats, SQL.",
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

const ENGINES: [&EngineSpec; 2] = [&DUCKDB, &SLOTHDB];

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

fn engine_dir(app_data: &Path, s: &EngineSpec) -> PathBuf {
    app_data.join("engines").join(s.id)
}

fn binary_path(app_data: &Path, s: &EngineSpec) -> PathBuf {
    engine_dir(app_data, s).join(binary_file_name(s))
}

/// Public helper kept for the engine() resolver in lib.rs.
pub fn duckdb_path(app_data: &Path) -> PathBuf {
    binary_path(app_data, &DUCKDB)
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
                ("linux", "aarch64") => "duckdb_cli-linux-aarch64.zip",
                ("macos", _) => "duckdb_cli-osx-universal.zip",
                _ => return None,
            }
            .to_string(),
        ),
        // SlothDB mirrors the same naming; adjust if its releases differ.
        "slothdb" => {
            let plat = match (os, arch) {
                ("windows", "x86_64") => "windows-amd64",
                ("windows", "aarch64") => "windows-arm64",
                ("linux", "x86_64") => "linux-amd64",
                ("linux", "aarch64") => "linux-aarch64",
                ("macos", _) => "macos-universal",
                _ => return None,
            };
            Some(format!("slothdb-{}.zip", plat))
        }
        _ => None,
    }
}

#[derive(Debug, Serialize)]
pub struct EngineStatus {
    pub id: String,
    pub name: String,
    pub description: String,
    pub required: bool,
    pub installed: bool,
    pub version: Option<String>,
    pub path: Option<String>,
    pub available: bool,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum InstallProgress {
    Downloading { received: u64, total: Option<u64> },
    Extracting,
    Verifying,
    Done { path: String },
}

pub fn status(app_data: &Path) -> Vec<EngineStatus> {
    ENGINES
        .iter()
        .map(|s| {
            let path = binary_path(app_data, s);
            let installed = path.exists();
            EngineStatus {
                id: s.id.to_string(),
                name: s.name.to_string(),
                description: s.description.to_string(),
                required: s.required,
                installed,
                version: installed.then(|| s.version.to_string()),
                path: installed.then(|| path.to_string_lossy().to_string()),
                available: asset_for(s).is_some(),
            }
        })
        .collect()
}

/// Download + install any engine by id. Streams progress.
pub fn install<F: FnMut(InstallProgress)>(
    app_data: &Path,
    engine_id: &str,
    on_progress: F,
) -> Result<String, String> {
    let s = spec(engine_id).ok_or_else(|| format!("Unknown engine '{}'", engine_id))?;
    install_spec(app_data, s, on_progress)
}

fn install_spec<F: FnMut(InstallProgress)>(
    app_data: &Path,
    s: &EngineSpec,
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
    let url = format!(
        "https://github.com/{}/releases/download/v{}/{}",
        s.repo, s.version, asset
    );

    let dir = engine_dir(app_data, s);
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;

    let client = reqwest::blocking::Client::builder()
        .user_agent("duckle")
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

    on_progress(InstallProgress::Extracting);
    let target = binary_path(app_data, s);
    let want = binary_file_name(s);
    let reader = std::io::Cursor::new(buf);
    let mut archive = zip::ZipArchive::new(reader).map_err(|e| e.to_string())?;
    let mut extracted = false;
    for i in 0..archive.len() {
        let mut file = archive.by_index(i).map_err(|e| e.to_string())?;
        let name = file.name().to_string();
        let leaf = name.rsplit('/').next().unwrap_or(&name);
        if leaf.eq_ignore_ascii_case(&want) || leaf.eq_ignore_ascii_case(s.binary) {
            let mut out = std::fs::File::create(&target).map_err(|e| e.to_string())?;
            std::io::copy(&mut file, &mut out).map_err(|e| e.to_string())?;
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

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o755));
    }

    on_progress(InstallProgress::Verifying);
    let ok = std::process::Command::new(&target)
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    if !ok {
        return Err(format!("Installed {} binary failed to run (--version)", s.name));
    }

    let path = target.to_string_lossy().to_string();
    on_progress(InstallProgress::Done { path: path.clone() });
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_lists_both_engines_missing_in_empty_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let st = status(tmp.path());
        assert_eq!(st.len(), 2);
        let duck = st.iter().find(|e| e.id == "duckdb").unwrap();
        assert!(!duck.installed && duck.required && duck.available);
        let sloth = st.iter().find(|e| e.id == "slothdb").unwrap();
        assert!(!sloth.installed && !sloth.required);
    }

    #[test]
    #[ignore = "downloads the DuckDB CLI from GitHub releases (network)"]
    fn installs_duckdb() {
        let tmp = tempfile::tempdir().unwrap();
        let path = install(tmp.path(), "duckdb", |_| {}).expect("install");
        assert!(std::path::Path::new(&path).exists());
        assert!(status(tmp.path())
            .iter()
            .any(|e| e.id == "duckdb" && e.installed));
    }
}
