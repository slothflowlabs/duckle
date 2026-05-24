# Roadmap - what is and isn't shipped

This document is the source of truth for what's in the palette but
not yet executable. The README's capability tables are the highlight
reel; this is the full ledger.

The palette currently carries **298 components**, broken down:

- **243 available** - executes on the DuckDB engine today
- **15 preview** - configurable in the designer (drag, wire, set
  properties); execution is being wired engine-by-engine
- **40 planned** - reserved in the palette so the roadmap is visible,
  not yet executable

If you drop a planned or preview tile and try to run, the executor
fails fast with `'<id>' isn't executable on the DuckDB engine yet -
it's a preview component.` rather than silently doing nothing.

---

## Planned, grouped by what's blocking them

### Streaming connectors (broker drivers)

| Component | Notes |
|---|---|
| `src.kafka` / `snk.kafka` | Needs `rdkafka` (C library) or a pure-Rust client; non-trivial dep |
| `src.pulsar` / `snk.pulsar` | Apache Pulsar Rust client; new dep |
| `src.redpanda` | Kafka-wire-compatible - alias of `src.kafka` once the driver lands |
| `src.nats` / `snk.nats` | `async-nats` driver; pairs with the streaming-pipeline mode |
| `src.kinesis` / `snk.kinesis` | AWS SDK for Rust; sizeable dep tree |
| `src.eventhubs` | Azure AMQP via `azure_sdk_eventhubs` |
| `src.pubsub` | GCP Pub/Sub Rust SDK |
| `src.rabbit` | `lapin` driver; AMQP 0.9.1 |

These need more than a driver - the engine's per-stage shell-out model
assumes a finite input. A streaming-source mode (continuous run with
checkpoint commits) is a separate engine workstream that lands alongside
the first broker driver.

### Vector-DB read sources (vendor-specific scan APIs)

| Component | Notes |
|---|---|
| `src.pinecone` | Pinecone has no "scan all vectors" API by design; reads happen via query (similarity search). The right shape is a query node, not a generic read source |
| `src.qdrant` | `POST /collections/{id}/points/scroll` - cursor pagination; doable as a thin wrapper |
| `src.weaviate` | `GET /v1/objects?class=X&limit=&after=` - cursor; doable |
| `src.chroma` / `snk.chroma` | Chroma's API is in flux; pin to a stable version first |
| `src.milvus` | `POST /v1/vector/query` with filter; doable |
| `src.lancedb` / `snk.lancedb` | LanceDB Rust SDK; new dep |

The corresponding **sinks** are all available today via vendor HTTP APIs:
Pinecone, Qdrant, Weaviate, Milvus.

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
| `src.ftp` | SFTP / FTP read - DuckDB already reads HTTPS via httpfs; FTP is rarer |
| `src.webhook` | Inbound HTTP listener - Duckle is desktop-only, not a server; needs a tunneling story |
| `src.git` | libgit2 - file-from-repo source; tractable but niche |
| `src.clipboard` | Desktop-only, niche |
| `src.airtable` / `src.notion` etc. | All **available** via the simple-auth REST aliases - see SaaS section in the README |

### NoSQL

| Component | Notes |
|---|---|
| `src.dynamodb` | AWS SDK for Rust; non-trivial |

`src.redis`, `snk.redis`, and `src.couchdb` shipped - see the
Capabilities table in the README.

### File formats

| Component | Notes |
|---|---|
| `src.avro` / `snk.avro` | DuckDB community `avro` extension is on v1.3 and needs a v1.5 release for Linux x64; tile stays preview until the extension publishes |
| `src.orc` / `snk.orc` | Apache ORC reader; no native DuckDB extension |
| `src.xml` / `snk.xml` | `quick-xml` - tractable; XML pulls in schema-validation scope |
| `src.yaml` | `serde_yaml`; trivial wrapper, on the short list |
| `src.toml` | `toml` crate; trivial wrapper, on the short list |
| `src.fixedwidth` | Positional column read; tractable via `read_csv` with regex |

### Custom-code stages

| Component | Notes |
|---|---|
| `code.python` | Sandboxing + Python embedding scope decision |
| `code.javascript` | V8 or QuickJS embed; sandboxing |
| `code.rust` | Cranelift JIT or Wasm; requires runtime crate |
| `code.wasm` | `wasmtime`; tractable, sandboxing-friendly |
| `code.shell` | Security boundary - shell execution from a desktop app needs careful UX |

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
| `src.soap` | Workaround: use `src.rest` with a POST body of the SOAP envelope until a real component lands |
| `src.odata` | OData v4 client; thin wrapper over src.rest is workable today |

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
