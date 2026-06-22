# Runtime Overview

This note maps the runtime path from frontend workflow graph to executed DuckDB/runtime stages.

Primary code:

- Frontend pipeline shape: `frontend/src/pipeline-types.ts`
- Tauri bridge/commands: `frontend/src/tauri-bridge.ts`, `apps/desktop/src/commands.rs`
- Engine: `crates/duckdb-engine/src/lib.rs`
- Planner: `crates/duckdb-engine/src/plan/mod.rs`
- SQL builders: `crates/duckdb-engine/src/plan/builders.rs`
- Runtime spec types: `crates/duckdb-engine/src/plan/specs.rs`

## High-Level Flow

```text
React canvas workflow
  -> Tauri command
  -> PipelineDoc { nodes, edges }
  -> planner compile
  -> ordered Stage list
  -> DuckDB CLI SQL execution and/or RuntimeSpec execution
  -> previews, logs, history, artifacts, side effects
```

## Key Runtime Objects

| Object | Location | Purpose |
|---|---|---|
| `PipelineDoc` | `plan/mod.rs` | Run payload containing nodes and edges. |
| `Stage` | `plan/mod.rs` | Executable unit: SQL, kind, source/sink metadata, runtime spec, retries, wait, memory limit. |
| `RuntimeSpec` | `plan/mod.rs` / `plan/specs.rs` | Non-SQL action for runtime-backed source/sink/transform/control nodes. |
| `DuckdbEngine` | `lib.rs` | CLI-driven engine that runs DuckDB SQL and runtime specs. |
| `EngineError` | `lib.rs` | Config/unsupported/query/cancelled/other error categories. |

## Runtime Split

Every stage is either:

| Stage type | Meaning |
|---|---|
| Pure SQL | `stage.runtime == None`; executable through DuckDB CLI SQL. |
| Runtime-backed | `stage.runtime == Some(...)`; executor performs Rust-side behavior, external API/driver calls, code execution, or control side effect. |

Pure SQL stages can be batched into fewer DuckDB CLI invocations when no hooks prevent it. Runtime-backed stages force per-stage handling.

## DuckDB Model

The engine uses the official DuckDB CLI binary, not an in-process DuckDB library.

```text
DuckdbEngine
  -> duckdb <temp-run-db> -json -bail -c "<sql>"
```

Runs use a temporary on-disk `.duckdb` database so separate CLI invocations can share intermediate tables.

See also:

- `docs/00_foundation/03_duckdb-cli-execution-model.md`
- `docs/03_runtime/03_duckdb-cli-and-sql-batching.md`

## Agent Rules

- First classify a node as pure SQL or runtime-backed.
- Debug pure SQL nodes through generated SQL/builders.
- Debug runtime-backed nodes through `RuntimeSpec` extraction and executor handling.
- Treat sinks and control/code nodes as side-effect boundaries.
- When adding a node, decide early whether it belongs in SQL builders or runtime specs.
