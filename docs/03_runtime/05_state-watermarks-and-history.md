# State, Watermarks, and History

Status: placeholder.

Purpose: document persisted runtime state, watermarks, run history, and state advancement rules.

Expected coverage:

- `xf.incremental` watermark persistence.
- `src.ducklake.changes` consumed snapshot persistence.
- Run history records.
- Success-only state advancement.
- Workspace state location.

Relevant code:

- `crates/duckdb-engine/src/watermark.rs`
- `crates/duckdb-engine/src/history.rs`
- `crates/duckdb-engine/src/run_log.rs`
- `crates/duckdb-engine/src/plan/specs.rs`

TODO:

- Inspect exact state file paths.
- Document state reset/replay behavior.
- Add operational guidance for local studio runs.
