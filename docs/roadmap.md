# Roadmap - what is and isn't shipped

This document is the source of truth for what's in the palette but
not yet executable. The README's capability tables are the highlight
reel; this is the full ledger.

The palette currently carries **446 components**, broken down:

- **441 available** - executes on the DuckDB engine today
- **5 planned** - reserved in the palette so the roadmap is visible,
  not yet executable

If you drop a planned or preview tile and try to run, the executor
fails fast with `'<id>' isn't executable on the DuckDB engine yet -
it's a preview component.` rather than silently doing nothing.

---

## Planned, grouped by what's blocking them

### Streaming connectors (broker drivers)

`src.kafka`, `snk.kafka`, `src.redpanda`, `snk.redpanda` shipped via
the pure-Rust `rskafka` driver (no C dep, builds cleanly on every
CI runner). `src.eventhubs` shipped on the same read, over an Event
Hubs namespace's Kafka endpoint and signed in with its connection
string, so it needs no AMQP client. `snk.kinesis` shipped on the
SigV4 signing `src.kinesis` reads with: PutRecords, with the records a
shard refuses sent again. `src.nats`, `snk.nats` shipped via `async-nats`.
`src.pubsub`, `snk.pubsub` shipped via direct REST calls
(sidestepping the gRPC build requirement of the official Google
client). `src.rabbit`, `snk.rabbit` shipped via the pure-Rust
`lapin` AMQP 0.9.1 driver. `src.pulsar`, `snk.pulsar` shipped over Pulsar's
WebSocket API, which needs no `protoc`. Current semantics across all of them
are **batch**: produce/consume up to N records per stage run. A
true streaming mode (continuous run with checkpoint commits) is a
separate engine workstream; lands alongside the next broker driver.

### Vector-DB read sources (vendor-specific scan APIs)

| Component | Notes |
|---|---|
| `src.lancedb` / `snk.lancedb` | LanceDB Rust SDK; new dep |

`src.qdrant`, `src.weaviate`, `src.milvus`, `src.chroma` and `src.pinecone` shipped - each is a
vendor-specific paginated HTTP scan implemented directly against the
public REST API (no SDK). See the Capabilities table in the README.
Corresponding **sinks** are all available too.

### OAuth-heavy SaaS

`src.gsheets` shipped without a Google SDK: a service account's key signs a
JWT (RS256) that is traded for a token on each run, or an API key reads a
sheet anyone with the link can view. `src.excel-online` shipped as an app
registration (client credentials): Graph's workbook API takes no app-only
token, so the workbook is downloaded and read as `src.excel` reads one.
`src.email` signs in with OAuth2 (XOAUTH2) as well as a password: a refresh
token traded for an access token each run, or an app's client credentials for
Microsoft 365.

The simple-auth SaaS REST tiles (GitHub, GitLab, Notion, Airtable, Stripe,
HubSpot, Jira, etc.) ship today through the generic `src.rest` path.
OAuth-heavy vendors need a stored-credential flow + token-refresh worker.

### Generic SaaS deferrals

| Component | Notes |
|---|---|
| `src.jdbc` / `snk.jdbc` | Generic JDBC bridges to Java - design question whether to bundle a JVM, ship a separate sidecar, or skip; deferred until a real user need |
| `src.couchdb` | Standard REST source pattern; not yet wrapped |
| `src.sftp` | SFTP read (SSH-based, different protocol from FTP) - separate component, requires russh-sftp or ssh2; not yet shipped |
| `src.airtable` / `src.notion` etc. | All **available** via the simple-auth REST aliases - see SaaS section in the README |

`src.git` shipped via shell-out to the system `git` CLI (no libgit2
dep, no extra Rust crate). Two modes: `log` emits commit history,
`files` emits the tracked-file tree at a revision. See the
Capabilities table in the README.

### NoSQL

| Component | Notes |
|---|---|

`src.redis`, `snk.redis`, and `src.couchdb` shipped - see the
Capabilities table in the README.

### File formats

| Component | Notes |
|---|---|
| `src.xml` (large + compressed) | `src.xml` ships today but reads the whole file into memory and does not decompress, so multi-GB XML and `.gz` / `.zip` inputs are not supported. Planned: a streaming pull-parser (row-by-row, bounded memory) plus gzip / zip decompression, so a large compressed XML can be read (or converted to Parquet/JSONL) without loading it all into RAM. CSV / JSON / Parquet already stream out-of-core and read `.gz` via DuckDB; big XML is the gap. |

`src.yaml`, `snk.yaml`, `src.toml`, `snk.toml`, `src.fixedwidth`,
`src.avro`, `snk.avro`, `src.xml`, `snk.xml`, `src.orc`, `snk.orc` shipped - see the
Capabilities table in the README.

### Custom-code stages

| Component | Notes |
|---|---|
| `code.python` | Sandboxing + Python embedding scope decision |
| `code.rust` | Cranelift JIT or Wasm; requires runtime crate |

`code.sql` and `code.sqltemplate` are **available** today (run user SQL
as a real materialized stage; the upstream is exposed as `input`).

### AI / LLM transforms

| Component | Notes |
|---|---|

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

### Enterprise Identity & Access Management (SSO / MFA)

OIDC single sign-on, IdP group-to-role mapping and MFA enforcement all ship
today. See [Enterprise SSO and MFA](security/reference/sso-okta-architecture-spec.md).
What remains on this list is SAML alone.

| Capability | Notes |
|---|---|
| `auth.saml` | SAML 2.0 Web Browser SSO. Not planned: every provider this would target (Entra ID, Okta, Google Workspace, Ping, Auth0, Keycloak) speaks OIDC natively, and OIDC is the smaller attack surface. A deployment that must use SAML can terminate it in a SAML-to-OIDC gateway. |

---

## What's `preview` and why

The 15 preview components are:

- **`src.avro`** - waiting on the DuckDB `avro` community extension to
  publish a v1.5-compatible binary.
- **`src.lancedb`, `snk.lancedb`** -
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
2. Define the spec in `crates/duckdb-engine/src/plan/specs.rs` and add a
   `RuntimeSpec` variant + a routing OR-arm in
   `crates/duckdb-engine/src/plan/mod.rs`.
3. Add the executor in `crates/duckdb-engine/src/connectors.rs` and a dispatch
   arm in `crates/duckdb-engine/src/lib.rs`.
4. Add a palette tile in `frontend/src/workflow-ui/palette-data.ts` (plus a field
   manifest in `frontend/src/workflow-ui/fields/manifest-synth.ts` when the node
   needs a custom property panel), then regenerate the catalog:
   `node frontend/scripts/build-catalog.mjs`.
5. Add an integration test in `crates/duckdb-engine/tests/execution.rs` -
   real network tests should be `env-gated` (skip unless the relevant
   `DUCKLE_*` env var is set); a mock HTTP server keeps a connector test in CI.
6. Update the README capability table and this roadmap.

See `CONTRIBUTING.md` for the broader project conventions.
