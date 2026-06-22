# Planner and Stage Compilation

This note explains how workflow nodes become executable stages.

Authoritative code:

- `crates/duckdb-engine/src/plan/mod.rs`
- `crates/duckdb-engine/src/plan/builders.rs`
- `crates/duckdb-engine/src/plan/graph.rs`
- `crates/duckdb-engine/src/plan/specs.rs`

## Input

The planner receives:

```rust
PipelineDoc {
    nodes: Vec<PipelineNode>,
    edges: Vec<PipelineEdge>,
}
```

The frontend produces the same logical structure from the canvas.

## Compilation Responsibilities

| Planner area | Responsibility |
|---|---|
| Graph compiler | Sort nodes, map edges to ports, identify inputs/outputs, validate columns where possible. |
| SQL builders | Convert SQL-backed components into DuckDB SQL fragments. |
| Runtime spec extraction | Convert non-SQL components into `RuntimeSpec` variants. |
| Stage metadata | Record sink path/mode, upstream relation, retries, waits, memory limit, attach-view flags. |
| Branch/control handling | Handle switch, parallelize, runjob, foreach, iterate, fallback, logs, die. |

## Component Dispatch

The planner dispatches by `component_id`.

Examples:

| Component pattern | Main location |
|---|---|
| SQL-backed sources/transforms/quality/sinks | `build_view_sql` / `build_sink_sql` in `builders.rs` |
| REST/API/runtime source/sink nodes | runtime branch in `plan/mod.rs` |
| Control nodes | `plan/mod.rs` and selected builder helpers |
| Runtime transforms | `plan/mod.rs` plus executor handling |
| Runtime specs | Data structs in `plan/specs.rs` |

## Stage Shape

Each compiled `Stage` includes:

| Field | Meaning |
|---|---|
| `node_id` | Instance id from workflow. |
| `component_id` | Node type, e.g. `src.csv`. |
| `label` | User-facing label. |
| `sql` | DuckDB SQL for pure SQL or pass-through/placeholder SQL. |
| `kind` | Stage kind. |
| `from` | Upstream relation used for counts/sinks/runtime stages. |
| `sink_path`, `sink_mode` | Used by sinks and output overwrite checks. |
| `runtime` | Optional non-SQL behavior. |
| `wait_ms`, `retry_attempts`, `retry_backoff_ms`, `memory_limit_mb` | Advanced execution settings. |
| `attach_view` | Optimization flag for selected attach-backed sources. |

## Schema Propagation

`plan/graph.rs` derives output column sets where safe.

| Behavior | Examples |
|---|---|
| Exact pass-through | filter, sort, sample, cast, fill operations. |
| Derived schema | drop, rename, project. |
| Unknown schema | joins, aggregates, pivots, custom SQL, column-adding transforms. |

Returning unknown schema is preferred over returning wrong schema.

## Adding a Node

For any new node:

1. Add palette entry.
2. Add form manifest.
3. Add SQL builder or runtime spec.
4. Add planner dispatch branch.
5. Add executor handling if runtime-backed.
6. Add schema propagation only when exact.
7. Add column validation for upstream column props.
8. Add tests.
9. Update docs.

## Agent Rules

- Do not infer runtime behavior from palette text alone.
- For raw workflow JSON, inspect planner prop extraction.
- Add a `RuntimeSpec` when behavior cannot be represented as DuckDB SQL.
- Keep impossible states out of `Stage`: prefer one runtime enum variant over many loosely-related optional fields.
