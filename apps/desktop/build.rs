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
    embed_runner_linux();
    embed_mcp();
    embed_lance();
    embed_delta();

    tauri_build::build()
}

/// zstd-compress `src` into `dst` so the embedded sidecar ships small; the app
/// inflates it on first use (inflate_embedded in lib.rs). Level 19 favors ratio
/// (decompression speed is level-independent). Panics on IO/compress error so a
/// broken embed fails the build loudly rather than shipping a corrupt sidecar.
fn compress_to(src: &std::path::Path, dst: &std::path::Path) {
    // build.rs re-runs on every build (to restamp the epoch), so skip recompressing
    // ~135MB of sidecars when the output is already newer than the source. A changed
    // sidecar re-triggers via its rerun-if-changed and is newer than dst, so it
    // recompresses; a clean build has no dst and compresses.
    if let (Ok(sm), Ok(dm)) = (
        src.metadata().and_then(|m| m.modified()),
        dst.metadata().and_then(|m| m.modified()),
    ) {
        if dm >= sm {
            return;
        }
    }
    let raw = std::fs::read(src).unwrap_or_else(|e| panic!("read {}: {}", src.display(), e));
    let comp = zstd::encode_all(std::io::Cursor::new(&raw), 19)
        .unwrap_or_else(|e| panic!("zstd compress {}: {}", src.display(), e));
    std::fs::write(dst, &comp).unwrap_or_else(|e| panic!("write {}: {}", dst.display(), e));
}

/// Locate a prebuilt `duckle-lance` (the LanceDB sidecar) and expose its bytes
/// via include_bytes!(env!("DUCKLE_EMBEDDED_LANCE")). OPTIONAL, and deliberately
/// NOT built here: lancedb needs protoc + pulls DataFusion, so the desktop build
/// must never compile it. CI builds it separately (with protoc) and stages it to
/// apps/desktop/bin/. When absent we embed an empty file so the desktop still
/// builds; src.lancedb / snk.lancedb then fall back to a duckle-lance on PATH or
/// DUCKLE_LANCE_BIN at runtime.
fn embed_lance() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    let out_dir = std::env::var("OUT_DIR").expect("OUT_DIR");
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let name = if target_os == "windows" {
        "duckle-lance.exe"
    } else {
        "duckle-lance"
    };
    let staged = std::path::Path::new(&manifest_dir).join("bin").join(name);
    let dst = std::path::Path::new(&out_dir).join("embedded-lance.bin");
    if staged.exists() {
        compress_to(&staged, &dst);
    } else {
        std::fs::write(&dst, [])
            .unwrap_or_else(|e| panic!("write empty embedded-lance: {}", e));
        println!(
            "cargo:warning=duckle-lance not staged (apps/desktop/bin/{name}); LanceDB nodes will need a duckle-lance on PATH or DUCKLE_LANCE_BIN. CI stages it."
        );
    }
    println!("cargo:rustc-env=DUCKLE_EMBEDDED_LANCE={}", dst.display());
    println!("cargo:rerun-if-changed={}", staged.display());
}

/// Locate a prebuilt `duckle-delta` (the Delta Lake sidecar) and expose its bytes
/// via include_bytes!(env!("DUCKLE_EMBEDDED_DELTA")). OPTIONAL and NOT built here,
/// for duckle-lance's reason: delta-rs pulls DataFusion, and it lives in its own
/// workspace (crates/duckle-delta). CI builds it and stages it to apps/desktop/bin/.
/// When absent an empty file is embedded so the desktop still builds; snk.delta's
/// upsert / update / delete and object-storage writes then fall back to a
/// duckle-delta on PATH or DUCKLE_DELTA_BIN.
fn embed_delta() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    let out_dir = std::env::var("OUT_DIR").expect("OUT_DIR");
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let name = if target_os == "windows" {
        "duckle-delta.exe"
    } else {
        "duckle-delta"
    };
    let staged = std::path::Path::new(&manifest_dir).join("bin").join(name);
    let dst = std::path::Path::new(&out_dir).join("embedded-delta.bin");
    if staged.exists() {
        compress_to(&staged, &dst);
    } else {
        std::fs::write(&dst, [])
            .unwrap_or_else(|e| panic!("write empty embedded-delta: {}", e));
        println!(
            "cargo:warning=duckle-delta not staged (apps/desktop/bin/{name}); Delta upsert/update/delete and object-storage writes will need a duckle-delta on PATH or DUCKLE_DELTA_BIN. CI stages it."
        );
    }
    println!("cargo:rustc-env=DUCKLE_EMBEDDED_DELTA={}", dst.display());
    println!("cargo:rerun-if-changed={}", staged.display());
}

/// Locate a freshly built `duckle-mcp` and expose its bytes to lib.rs via
/// include_bytes!(env!("DUCKLE_EMBEDDED_MCP")). Unlike the runner (required for
/// Build Pipeline), the MCP server is optional: when it is not staged we embed
/// an empty file so the desktop still builds, and the in-app MCP popup reports
/// that this build carries no bundled server. CI / release stage it for real.
fn embed_mcp() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    let out_dir = std::env::var("OUT_DIR").expect("OUT_DIR");
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let name = if target_os == "windows" {
        "duckle-mcp.exe"
    } else {
        "duckle-mcp"
    };

    let staged = std::path::Path::new(&manifest_dir).join("bin").join(name);
    let profile_dir = std::path::Path::new(&out_dir)
        .ancestors()
        .nth(3)
        .map(|p| p.join(name));
    let source = if staged.exists() {
        Some(staged)
    } else {
        profile_dir.filter(|p| p.exists())
    };

    let dst = std::path::Path::new(&out_dir).join("embedded-mcp.bin");
    match source {
        Some(src) => {
            compress_to(&src, &dst);
            println!("cargo:rerun-if-changed={}", src.display());
        }
        None => {
            std::fs::write(&dst, [])
                .unwrap_or_else(|e| panic!("write empty embedded-mcp: {}", e));
            println!(
                "cargo:warning=duckle-mcp not staged (apps/desktop/bin/{name}); the in-app MCP popup will report no bundled server. Stage it: cargo build --profile release-runner -p duckle-mcp"
            );
        }
    }
    println!("cargo:rustc-env=DUCKLE_EMBEDDED_MCP={}", dst.display());
    println!(
        "cargo:rerun-if-changed={}",
        std::path::Path::new(&manifest_dir).join("bin").join(name).display()
    );
}

/// Locate the prebuilt STATIC Linux duckle-runner and expose its bytes to
/// lib.rs via include_bytes!(env!("DUCKLE_EMBEDDED_RUNNER_LINUX")). This is the
/// stub the desktop prepends when "Build Pipeline" targets Linux from a
/// non-Linux host (cross-OS build). It is produced by
/// scripts/build-runner-linux.sh (Docker musl build) and staged, gitignored,
/// at apps/desktop/bin/duckle-runner-linux-x64.
///
/// Unlike the host runner (required), this is OPTIONAL: when not staged we
/// embed an empty file so the desktop still builds; the Build Pipeline command
/// then reports that this build cannot target Linux. On a Linux host build the
/// host runner already covers the Linux target, so the cross stub is not staged
/// there either.
fn embed_runner_linux() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    let out_dir = std::env::var("OUT_DIR").expect("OUT_DIR");

    let staged = std::path::Path::new(&manifest_dir)
        .join("bin")
        .join("duckle-runner-linux-x64");
    let dst = std::path::Path::new(&out_dir).join("embedded-runner-linux.bin");
    if staged.exists() {
        compress_to(&staged, &dst);
    } else {
        std::fs::write(&dst, [])
            .unwrap_or_else(|e| panic!("write empty embedded-runner-linux: {}", e));
        println!(
            "cargo:warning=Linux runner not staged (apps/desktop/bin/duckle-runner-linux-x64); Build Pipeline will not be able to target Linux from this build. Stage it: bash scripts/build-runner-linux.sh"
        );
    }
    println!("cargo:rustc-env=DUCKLE_EMBEDDED_RUNNER_LINUX={}", dst.display());
    println!("cargo:rerun-if-changed={}", staged.display());
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
    compress_to(&source, &dst);

    println!("cargo:rustc-env=DUCKLE_EMBEDDED_RUNNER={}", dst.display());
    println!(
        "cargo:rerun-if-changed={}",
        std::path::Path::new(&manifest_dir).join("bin").join(name).display()
    );
    println!("cargo:rerun-if-changed={}", source.display());
}
