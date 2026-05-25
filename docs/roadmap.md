# Roadmap - what is and isn't shipped

This document is the source of truth for what's in the palette but
not yet executable. The README's capability tables are the highlight
reel; this is the full ledger.

The palette currently carries **303 components**, broken down:

- **270 available** - executes on the DuckDB engine today
- **11 preview** - configurable in the designer (drag, wire, set
  properties); execution is being wired engine-by-engine
- **22 planned** - reserved in the palette so the roadmap is visible,
  not yet executable

If you drop a planned or preview tile and try to run, the executor
fails fast with `'<id>' isn't executable on the DuckDB engine yet -
it's a preview component.` rather than silently doing nothing.

---

## Planned, grouped by what's blocking them

### Streaming connectors (broker drivers)

| Component | Notes |
|---|---|
| `src.pulsar` / `snk.pulsar` | The `pulsar` Rust crate compiles its protobuf definitions with `prost-build` which requires `protoc` to be installed at build time. That breaks the self-contained build. Options: vendor protoc via `protoc-bin-vendored`; hand-roll the subset of Pulsar protobuf ops we need; wait for an alt crate. **Deferred until a build approach lands.** Pulsar has no REST data plane to fall back on. |
| `src.kinesis` / `snk.kinesis` | AWS SDK for Rust; sizeable dep tree |
| `src.eventhubs` | Azure AMQP via `azure_sdk_eventhubs` |

`src.kafka`, `snk.kafka`, `src.redpanda`, `snk.redpanda` shipped via
the pure-Rust `rskafka` driver (no C dep, builds cleanly on every
CI runner). `src.nats`, `snk.nats` shipped via `async-nats`.
`src.pubsub`, `snk.pubsub` shipped via direct REST calls
(sidestepping the gRPC build requirement of the official Google
client). `src.rabbit`, `snk.rabbit` shipped via the pure-Rust
`lapin` AMQP 0.9.1 driver. Current semantics across all of them
are **batch**: produce/consume up to N records per stage run. A
true streaming mode (continuous run with checkpoint commits) is a
separate engine workstream; lands alongside the next broker driver.

### Vector-DB read sources (vendor-specific scan APIs)

| Component | Notes |
|---|---|
| `src.pinecone` | Pinecone has no "scan all vectors" API by design; reads happen via query (similarity search). The right shape is a query node, not a generic read source |
| `src.chroma` / `snk.chroma` | Chroma's API is in flux; pin to a stable version first |
| `src.lancedb` / `snk.lancedb` | LanceDB Rust SDK; new dep |

`src.qdrant`, `src.weaviate`, and `src.milvus` shipped - each is a
vendor-specific paginated HTTP scan implemented directly against the
public REST API (no SDK). See the Capabilities table in the README.
Corresponding **sinks** are all available too.

### OAuth-heavy SaaS

| Component | Notes |
|---|---|
| `src.gsheets` | Needs full OAuth 2.0 flow + Google Workspace SDK |
| `src.excel-online` | Microsoft Graph API + OAuth |

The simple-auth SaaS REST tiles (GitHub, GitLab, Notion, Airtable, Stripe,
HubSpot, Jira, etc.) ship today through the generic `src.rest` path.
OAuth-heavy vendors need a stored-credential flow + token-refresh worker.

### Generic SaaS deferrals

| Component | Notes |
|---|---|
| `src.jdbc` / `snk.jdbc` | Generic JDBC bridges to Java - design question whether to bundle a JVM, ship a separate sidecar, or skip; deferred until a real user need |
| `src.couchdb` | Standard REST source pattern; not yet wrapped |
| `src.email` | IMAP/POP3 - more useful as a trigger source than a batch read |
| `src.sftp` | SFTP read (SSH-based, different protocol from FTP) - separate component, requires russh-sftp or ssh2; not yet shipped |
| `src.webhook` | Inbound HTTP listener - Duckle is desktop-only, not a server; needs a tunneling story |
| `src.clipboard` | Desktop-only, niche |
| `src.airtable` / `src.notion` etc. | All **available** via the simple-auth REST aliases - see SaaS section in the README |

`src.git` shipped via shell-out to the system `git` CLI (no libgit2
dep, no extra Rust crate). Two modes: `log` emits commit history,
`files` emits the tracked-file tree at a revision. See the
Capabilities table in the README.

### NoSQL

| Component | Notes |
|---|---|
| `src.dynamodb` | AWS SDK for Rust; non-trivial |

`src.redis`, `snk.redis`, and `src.couchdb` shipped - see the
Capabilities table in the README.

### File formats

| Component | Notes |
|---|---|
| `src.orc` / `snk.orc` | Apache ORC reader; no native DuckDB extension; the `orc` Rust crate exists but is minimal |

`src.yaml`, `snk.yaml`, `src.toml`, `snk.toml`, `src.fixedwidth`,
`src.avro`, `snk.avro`, `src.xml`, `snk.xml` shipped - see the
Capabilities table in the README.

### Custom-code stages

| Component | Notes |
|---|---|
| `code.python` | Sandboxing + Python embedding scope decision |
| `code.javascript` | V8 or QuickJS embed; sandboxing |
| `code.rust` | Cranelift JIT or Wasm; requires runtime crate |
| `code.wasm` | `wasmtime`; tractable, sandboxing-friendly |

`code.sql` and `code.sqltemplate` are **available** today (run user SQL
as a real materialized stage; the upstream is exposed as `input`).

### AI / LLM transforms

| Component | Notes |
|---|---|
| `xf.ai.embed` | OpenAI / Cohere / Voyage / local-model embed call |
| `xf.ai.llm` | LLM transform - per-row prompt + parse response |
| `xf.ai.chunk` | Text splitter (recursive / semantic / token-based) |
| `xf.ai.classify` | LLM-backed classification |
| `xf.ai.pii` | LLM- or NER-backed PII redaction |
| `xf.ai.dedupe` | Embedding-based semantic dedupe |

All six need a **model API credential pattern** that doesn't yet exist
(the current credentials store handles cloud storage / DB auth, not API
keys for OpenAI-shaped vendors with rate limits and streaming responses).
The non-AI half of the search story (Vector Similarity Search + Full-Text
Search via DuckDB `vss` / `fts`) ships today.

### Quality

| Component | Notes |
|---|---|
| `qa.addressclean` | Needs `libpostal` or a hosted address normalization API |

### Control

| Component | Notes |
|---|---|
| `ctl.schedule` | Schedules exist - they're configured in the Schedule panel via the orchestration crate, not as a graph node. The graph-node form is on the roadmap for a "this pipeline triggers that pipeline" semantic |

### Other

| Component | Notes |
|---|---|
| `src.grpc` | Protobuf parsing + per-service tonic codegen; non-trivial |

---

## What's `preview` and why

The 15 preview components are:

- **`src.avro`** - waiting on the DuckDB `avro` community extension to
  publish a v1.5-compatible binary.
- **`src.pinecone`, `src.qdrant`, `src.weaviate`, `src.milvus`** -
  designer-side configurable, engine-side waiting on the vendor-specific
  scan-endpoint work (see vector-DB section above). All four have
  working **sinks** today.
- **`src.chroma`, `snk.chroma`, `src.lancedb`, `snk.lancedb`** -
  waiting on a vendor SDK pin or a stable HTTP API.
- **`xf.ai.embed`, `xf.ai.llm`, `xf.ai.chunk`, `xf.ai.classify`,
  `xf.ai.pii`, `xf.ai.dedupe`** - waiting on the model-API credential
  pattern.

If you drop a preview tile, the executor surfaces a clear error
(`'<id>' isn't executable on the DuckDB engine yet`) rather than
silently passing data through.

---

## What's NOT on the roadmap (intentional non-goals)

- **A distributed execution engine** - Duckle is a single-machine
  embedded studio. If a pipeline outgrows one box, the right answer is
  to land its output in the system that scales (a warehouse, an object
  store) rather than to scale Duckle itself.
- **A hosted SaaS** - the project is local-first by design. Future work
  on the orchestration crate may add a "run this pipeline on a remote
  Duckle daemon" mode, but not a hosted Duckle service.
- **A workflow visual debugger beyond what's already there** - the live
  per-stage preview, per-stage row counts, run-history, and mid-run
  cancel cover the inner-loop ETL debugging need; deeper time-travel
  debugging is not currently planned.

---

## Contributing a connector

The fastest path for a new connector is to:

1. Read an existing one as a template - `snk.pinecone` (~40 lines) for a
   vendor HTTP sink, `src.mongodb` (~60 lines + a tokio block_on
   pattern) for an async-driver source.
2. Add an OR-arm to the planner branch in `crates/duckdb-engine/src/plan.rs`.
3. Add an executor branch in `crates/duckdb-engine/src/lib.rs`.
4. Add a palette tile in `frontend/src/workflow-ui/palette-data.ts`.
5. Add an integration test in `crates/duckdb-engine/tests/execution.rs` -
   real network tests should be `env-gated` (skip unless the relevant
   `DUCKLE_*_URI` env var is set).
6. Update the README capability table and this roadmap.

See `CONTRIBUTING.md` for the broader project conventions.
