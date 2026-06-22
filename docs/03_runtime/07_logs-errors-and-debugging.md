# Logs, Errors, and Debugging

Status: placeholder.

Purpose: document runtime logs, error categories, and debugging paths.

Expected coverage:

- `EngineError` categories.
- Run log NDJSON/event shape.
- Preview rows and row-count capture.
- Error category mapping.
- Tauri/frontend display of failures.
- Debugging generated SQL vs runtime specs.

Relevant code/docs:

- `crates/duckdb-engine/src/error_category.rs`
- `crates/duckdb-engine/src/run_log.rs`
- `crates/duckdb-engine/src/lib.rs`
- `docs/02_workflows/11_debugging-failed-workflows.md`

TODO:

- Inspect run log schema.
- Add common error examples.
- Add local file paths for logs.
