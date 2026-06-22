# Files, Artifacts, and Workspaces

Status: placeholder.

Purpose: document where local runtime data lives.

Expected coverage:

- Workspace directories.
- Pipeline files.
- Run temp DB files.
- Checkpoints and artifact conventions.
- Managed DuckDB binary.
- Bundled/staged runner and MCP binaries.
- dbt/project artifacts.

Relevant code/docs:

- `docs/00_foundation/02_local-studio-quickstart.md`
- `apps/desktop/src/engine_manager.rs`
- `crates/duckdb-engine/src/lib.rs`
- `frontend/src/workspace.ts`

TODO:

- Inspect actual workspace file layout from a running app.
- Define recommended workspace-relative artifact paths.
- Document cleanup behavior.
