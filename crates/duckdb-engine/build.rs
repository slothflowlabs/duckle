// No native linking needed for the DuckDB path: the engine drives the DuckDB
// CLI instead of statically linking libduckdb.
//
// The one exception is the `teradata-static` feature on Linux (issue #131):
// odbc-sys's `static` / `static_ltdl` features switch its extern block to
// `#[link(name = "odbc", kind = "static")]` / `-lltdl`, but libodbc.a also
// needs libodbcinst plus dl/pthread, and the archives live in the multiarch
// dir. We emit those here so the Teradata ODBC driver manager is baked into the
// binary and the shipped Linux build needs no system libodbc.so.2 to launch.
// (Requires unixodbc-dev + libltdl-dev at build time.)
fn main() {
    let teradata_static = std::env::var_os("CARGO_FEATURE_TERADATA_STATIC").is_some();
    let is_linux = std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("linux");
    if teradata_static && is_linux {
        let arch = std::env::var("CARGO_CFG_TARGET_ARCH").unwrap_or_default();
        if let Ok(home) = std::env::var("HOME") {
            println!("cargo:rustc-link-search=native={home}/.local/lib");
        }
        if let Ok(static_path) = std::env::var("ODBC_SYS_STATIC_PATH") {
            println!("cargo:rustc-link-search=native={static_path}");
        }
        println!("cargo:rustc-link-search=native=/usr/local/lib");
        println!("cargo:rustc-link-search=native=/usr/lib/{arch}-linux-gnu");
        println!("cargo:rustc-link-search=native=/usr/lib64");
        println!("cargo:rustc-link-search=native=/usr/lib");
        println!("cargo:rustc-link-lib=static=odbcinst");
        println!("cargo:rustc-link-lib=dylib=dl");
        println!("cargo:rustc-link-lib=dylib=pthread");
    }
}

