# External Dependencies

Status: placeholder.

Purpose: document runtime dependencies outside the Rust/React codebase.

Expected coverage:

- Managed DuckDB CLI.
- DuckDB extensions.
- Tauri desktop prerequisites.
- `duckle-runner` staging.
- `duckle-mcp` staging.
- Oracle Instant Client.
- System `git` CLI.
- dbt runtime.
- Network services and ports.
- WSL/Linux display/GPU caveats.

Relevant docs:

- `docs/00_foundation/02_local-studio-quickstart.md`
- `docs/00_foundation/03_duckdb-cli-execution-model.md`

TODO:

- Consolidate known startup fixes.
- Add dependency matrix by node family.
- Add WSL-specific troubleshooting section.
