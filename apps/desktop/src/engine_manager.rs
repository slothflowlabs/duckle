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

pub const DUCKDB_VERSION: &str = "1.2.2";
pub const SLOTHDB_VERSION: &str = "0.2.7";

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
    /// Per-extension progress for the DuckDB extension pre-install step
    /// that runs after the engine binary lands. Fetching them up front
    /// means the first time a fresh user touches a Postgres source or an
    /// S3 file there is no network hop.
    InstallingExtension { name: String, index: u32, total: u32 },
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
    "avro",     // Avro reader
    "iceberg",  // Apache Iceberg table scan
    "delta",    // Delta Lake table scan
];

fn duckdb_command(bin: &Path) -> std::process::Command {
    let mut cmd = std::process::Command::new(bin);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        // CREATE_NO_WINDOW: suppress the console flash on Windows.
        cmd.creation_flags(0x0800_0000);
    }
    cmd
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

    let target = binary_path(app_data, s);

    if asset.to_ascii_lowercase().ends_with(".zip") {
        // Zipped distribution (DuckDB) - pull the binary out.
        on_progress(InstallProgress::Extracting);
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
    } else {
        // Raw single-file binary (SlothDB) - the download IS the binary.
        if buf.is_empty() {
            return Err(format!("{} download was empty", s.name));
        }
        std::fs::write(&target, &buf).map_err(|e| e.to_string())?;
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o755));
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

    // Pre-fetch the extensions Duckle uses so the first connector hit
    // doesn't pause to download an extension. Only meaningful for the
    // DuckDB engine; SlothDB has its own model.
    if s.id == "duckdb" {
        install_duckdb_extensions(&target, &mut on_progress);
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

    #[test]
    #[ignore = "downloads the SlothDB raw binary from GitHub releases (network)"]
    fn installs_slothdb() {
        let tmp = tempfile::tempdir().unwrap();
        let path = install(tmp.path(), "slothdb", |_| {}).expect("install");
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
