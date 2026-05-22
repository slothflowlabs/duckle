// No native linking needed now that the engine drives the DuckDB CLI
// instead of statically linking libduckdb. Kept as a no-op so Cargo
// doesn't complain about a stale build script reference.
fn main() {}
