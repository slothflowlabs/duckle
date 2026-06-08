fn main() {
    // Stamp the build time (unix seconds) into the binary so the running app
    // can compare itself to the latest GitHub release asset's upload time and
    // prompt the user to upgrade when a newer build is published (see
    // update_check.rs). In CI release builds the target is clean, so this
    // re-stamps to the build time of the shipped binary; for local incremental
    // builds it only re-runs when build.rs changes, which is fine - the update
    // check is a no-op for un-stamped / dev binaries.
    let epoch = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    println!("cargo:rustc-env=DUCKLE_BUILD_EPOCH={epoch}");
    // Force this script to re-run on EVERY build so the stamped epoch is always
    // the actual build time. Pinning rerun to build.rs alone left local rebuilds
    // carrying the very first build's timestamp, which made the update check
    // report "a newer build is available" even when the local build was newer
    // than the release. Referencing a path that never exists makes Cargo treat
    // the script as always-dirty and re-run it.
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=.duckle-always-restamp-build-epoch");

    embed_runner();

    tauri_build::build()
}

/// Locate a freshly built `duckle-runner` and expose its bytes to lib.rs via
/// include_bytes!(env!("DUCKLE_EMBEDDED_RUNNER")). The runner is captured at
/// desktop-compile time, so developers must build duckle-runner BEFORE (or
/// alongside) the desktop build. CI stages it to apps/desktop/bin/.
fn embed_runner() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    let out_dir = std::env::var("OUT_DIR").expect("OUT_DIR");
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let name = if target_os == "windows" {
        "duckle-runner.exe"
    } else {
        "duckle-runner"
    };

    // Candidate source order (first existing wins):
    //  1. <CARGO_MANIFEST_DIR>/bin/<name> - CI/local staged copy (PRIMARY;
    //     avoids guessing the profile dir).
    //  2. <profile-dir>/<name> - OUT_DIR is target/<profile>/build/<hash>/out,
    //     so the 3rd ancestor is target/<profile>. Do NOT hardcode
    //     release/debug; release-runner changes it. Dev fallback only.
    let staged = std::path::Path::new(&manifest_dir).join("bin").join(name);
    let profile_dir = std::path::Path::new(&out_dir)
        .ancestors()
        .nth(3)
        .map(|p| p.join(name));

    let source = if staged.exists() {
        staged
    } else if let Some(p) = profile_dir.filter(|p| p.exists()) {
        p
    } else {
        panic!(
            "duckle-runner not found for embedding. Build it first: cargo build --profile release-runner -p duckle-runner (CI stages it to apps/desktop/bin/)."
        );
    };

    let dst = std::path::Path::new(&out_dir).join("embedded-runner.bin");
    std::fs::copy(&source, &dst).unwrap_or_else(|e| {
        panic!("copy {} -> {}: {}", source.display(), dst.display(), e)
    });

    println!("cargo:rustc-env=DUCKLE_EMBEDDED_RUNNER={}", dst.display());
    println!(
        "cargo:rerun-if-changed={}",
        std::path::Path::new(&manifest_dir).join("bin").join(name).display()
    );
    println!("cargo:rerun-if-changed={}", source.display());
}
