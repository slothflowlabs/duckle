# Runtime Specs

This note documents the non-SQL runtime-spec model.

Authoritative code:

- Runtime enum: `crates/duckdb-engine/src/plan/mod.rs`
- Spec structs: `crates/duckdb-engine/src/plan/specs.rs`
- Spec extraction: `crates/duckdb-engine/src/plan/mod.rs`
- Execution handlers: `crates/duckdb-engine/src/lib.rs`

## Core Idea

When a node cannot be represented as pure DuckDB SQL, the planner creates a `RuntimeSpec`.

```text
node props + inputs
  -> planner extracts typed spec
  -> Stage.runtime = Some(RuntimeSpec::<variant>)
  -> executor handles variant in Rust
  -> output is materialized for downstream nodes when needed
```

## RuntimeSpec Categories

| Category | Examples |
|---|---|
| Control/orchestration | `RunJob`, `InstallFallback`, `Iterate`, `Foreach`, `Parallelize`, `Log`, `Die` |
| Stateful sources/transforms | `Incremental`, `DuckLakeCdc` |
| HTTP/API | `RestSource`, `Webhook`, Snowflake/Databricks source/sink specs |
| Database/runtime clients | SQL Server, Oracle, Cassandra, MongoDB, Redis, ClickHouse, ADBC |
| Messaging | Kafka, NATS, RabbitMQ, Pub/Sub, Kinesis |
| File/format runtime | XML, Avro, YAML/TOML, FTP/SFTP, email, clipboard, git |
| Code execution | Shell, JavaScript, WASM, dbt |
| AI/vector | AI embed/LLM/classify/chunk/PII/dedupe, Qdrant/Weaviate/Milvus |

## Terminal vs Side-Effect Specs

Runtime specs split into two broad behaviors:

| Behavior | Meaning |
|---|---|
| Terminal replacement | Runtime action replaces the stage SQL. Example: runtime source fetches data and materializes output. |
| Side effect plus SQL | Runtime action runs, then pass-through/placeholder SQL still matters. Example: `ctl.log`, `ctl.runjob`, `ctl.try`. |

The enum comments in `plan/mod.rs` explicitly call this out.

## Spec Extraction Rules

Spec extraction should:

- Validate required props early.
- Normalize aliases/backward-compatible prop names.
- Set safe defaults.
- Capture upstream relation names.
- Avoid doing external I/O except small local file reads needed for config, such as private key files.
- Return `EngineError::Config` for user-fixable configuration problems.

## Execution Rules

Runtime handlers should:

- Check cancellation in long loops.
- Respect retry behavior where the stage executor wraps them.
- Materialize output with stable table/view names when downstream nodes need rows.
- Surface clear config/auth/network errors.
- Avoid mutating state until the run is known successful when state advancement matters.

## Agent Debugging

For a runtime-backed node:

1. Find the `RuntimeSpec` variant in `plan/mod.rs`.
2. Find the spec struct in `plan/specs.rs`.
3. Find executor handling in `lib.rs`.
4. Check required props and defaults.
5. Check external dependencies.
6. Add a checkpoint before the node if upstream work is expensive.

## Adding a RuntimeSpec

Minimum checklist:

1. Add typed spec struct to `plan/specs.rs` if needed.
2. Add enum variant to `RuntimeSpec`.
3. Extract props in `plan/mod.rs`.
4. Set `Stage.runtime`.
5. Implement executor handling in `lib.rs`.
6. Add tests for config validation and execution behavior.
7. Update node contract docs.
