# DuckDB CLI and SQL Batching

Status: placeholder.

Purpose: deepen the existing DuckDB CLI execution-model notes into a runtime reference.

Start from:

- `docs/00_foundation/03_duckdb-cli-execution-model.md`
- `crates/duckdb-engine/src/lib.rs`
- `crates/duckdb-engine/src/plan/mod.rs`

Expected coverage:

- Managed DuckDB binary and `DUCKLE_DUCKDB_BIN`.
- `duckdb -json -bail -c`.
- Temp on-disk run DB.
- Pure SQL batching.
- Per-stage fallback.
- Extension prelude and attach/detach behavior.
- CLI cancellation and stderr/stdout handling.

TODO:

- Pull exact batched execution conditions from `lib.rs`.
- Document extension install/load paths.
- Add sample generated SQL sequence.
