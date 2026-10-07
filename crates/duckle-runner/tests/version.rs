//! `duckle-runner --version` names the release the binary was built from.

use std::process::Command;

/// The version a release sets: apps/desktop/tauri.conf.json is the file it
/// bumps, and every crate's own version is a 0.0.1 placeholder.
fn release_version() -> String {
    let conf = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../apps/desktop/tauri.conf.json");
    let conf: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(conf).unwrap()).unwrap();
    conf["version"].as_str().expect("tauri.conf.json has a version").to_string()
}

#[test]
fn version_names_the_release() {
    for flag in ["--version", "-V"] {
        let out = Command::new(env!("CARGO_BIN_EXE_duckle-runner")).arg(flag).output().unwrap();
        assert_eq!(out.status.code(), Some(0), "{flag}: {}", String::from_utf8_lossy(&out.stderr));
        assert_eq!(
            String::from_utf8_lossy(&out.stdout).trim(),
            format!("duckle-runner {}", release_version()),
            "{flag}"
        );
    }
}
