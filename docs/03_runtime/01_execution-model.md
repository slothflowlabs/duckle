# Execution Model

This note describes what happens when a workflow run starts.

Authoritative code:

- `crates/duckdb-engine/src/lib.rs`
- `crates/duckdb-engine/src/plan/mod.rs`
- `crates/duckdb-engine/src/plan/graph.rs`

## Run Lifecycle

```text
1. Receive PipelineDoc from frontend/runner.
2. Compile graph into ordered stages.
3. Create/use a run-scoped DuckDB database file.
4. Execute stages in topological order.
5. For each stage:
   - apply retry/wait/memory settings
   - run SQL or runtime spec
   - capture preview/count/log/history metadata
6. Persist successful state updates where applicable.
7. Return run result/events to caller.
```

## Stage Kinds

| Stage kind | Meaning |
|---|---|
| View/table stage | Produces an intermediate relation for downstream nodes. |
| Sink stage | Consumes upstream relation and writes external/local output. |
| Runtime stage | Uses `RuntimeSpec` to do non-SQL work. |
| Control stage | Usually pass-through plus orchestration/log/fail/checkpoint side effect. |

Exact enum names should be checked in `plan/mod.rs` before modifying runtime behavior.

## Materialization

The comments in the engine describe the runtime model as temp on-disk DuckDB materialization.

Important implications:

- Intermediate results can be shared across separate CLI invocations.
- Pure SQL stages can sometimes batch together.
- Runtime-backed stages often materialize rows before/after Rust-side processing.
- Sinks read from a named upstream relation.

## Cancellation

`DuckdbEngine` owns a cancellation flag.

- Top-level runs should use `for_new_run()` so cancellation state is not shared across runs.
- Long DuckDB SQL can be interrupted by killing the active CLI child process.
- Runtime loops check cancellation between pages/batches where implemented.

## Retry, Wait, and Memory Limits

Each `Stage` carries advanced execution settings:

| Setting | Meaning |
|---|---|
| `retry_attempts` | Total attempts for the stage. `1` means no retry. |
| `retry_backoff_ms` | Backoff between attempts, with linear scaling. |
| `wait_ms` | Delay before stage execution, set by `ctl.wait` / `ctl.throttle`. |
| `memory_limit_mb` | Adds a DuckDB memory limit pragma for that stage. |

Any new runtime behavior should respect cancellation and retry semantics where applicable.

## Side Effects

Side effects include:

- Writing files/databases/object storage.
- Sending HTTP requests, messages, or emails.
- Running shell/JS/WASM/dbt.
- Running child pipelines/jobs.
- Updating watermarks/state after successful runs.
- Writing logs/history.

Agent rule: validation and checkpoints should happen before expensive or irreversible side effects.

## Agent Debug Flow

1. Identify failing `component_id`.
2. Find compiled stage behavior.
3. Check `stage.runtime`.
4. If pure SQL, inspect builder branch and generated SQL.
5. If runtime-backed, inspect spec extraction in `plan/mod.rs` and executor handler in `lib.rs`.
6. Check cancellation/retry/wait/memory settings if behavior differs between runs.
