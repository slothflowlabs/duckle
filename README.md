<div align="center">

<img src="docs/assets/hero.svg" alt="Duckle" width="100%"/>

<h3>The local-first data studio. Drag, wire, run at native speed.</h3>

<p><b>Duckle</b> is an open-source, local-first <b>ETL / ELT studio</b>: a drag-and-drop pipeline designer that compiles your canvas to SQL and runs it on your machine through DuckDB. Read from files, databases, warehouses, SaaS APIs, NoSQL stores, message buses, and vector DBs; reshape with 50+ transforms; land clean data anywhere. Ships as a ~9 MB desktop app, no bundled database, no servers, no lock-in.</p>

<p>
<img alt="status" src="https://img.shields.io/badge/status-early%20development-orange"/>
<img alt="license" src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue"/>
<img alt="platforms" src="https://img.shields.io/badge/platforms-Windows%20%C2%B7%20macOS%20%C2%B7%20Linux-2b6cb0"/>
<img alt="rust" src="https://img.shields.io/badge/Rust-000000?logo=rust&logoColor=white"/>
<img alt="tauri" src="https://img.shields.io/badge/Tauri%202-24C8DB?logo=tauri&logoColor=white"/>
<img alt="react" src="https://img.shields.io/badge/React%2019-20232A?logo=react&logoColor=61DAFB"/>
<img alt="typescript" src="https://img.shields.io/badge/TypeScript-3178C6?logo=typescript&logoColor=white"/>
<img alt="duckdb" src="https://img.shields.io/badge/DuckDB-FFF000?logo=duckdb&logoColor=black"/>
<img alt="stars" src="https://img.shields.io/github/stars/SouravRoy-ETL/duckle?style=social"/>
</p>

</div>

---

## What is Duckle?

Most data tooling forces a choice: a heavyweight enterprise suite you have to host, or a pile of scripts you have to maintain. Duckle is the middle path, a visual studio that runs entirely on your machine and stays out of your way.

You build a pipeline by dragging nodes onto a canvas and wiring them together. Duckle compiles that graph into SQL and executes it on a real analytical engine. Nothing is hidden: click any node to read the **generated SQL** and see a **live preview** of the rows flowing through it.

<div align="center">
<img src="docs/assets/flow.svg" alt="Sources flow through 40+ transforms into files, databases, object storage, and AI stores" width="100%"/>
</div>

### Why Duckle is different

| | |
|---|---|
| **Visual, never opaque** | The canvas compiles to SQL you can read, and every node has a live preview tab. No black box. |
| **Tiny binary, no bundled DB** | The app is ~9 MB. The DuckDB engine downloads on first launch with a guided step, so installs stay small and updates stay fast. |
| **Native speed** | Execution runs through DuckDB: vectorized, columnar, local. A clean-and-export job that crawls in a spreadsheet finishes in milliseconds. |
| **Git-friendly by design** | Pipelines, connections, contexts, and routines persist as plain files in a folder you pick. Diff them, branch them, review them. |
| **Honest about scope** | Single-machine and embedded by design. Built to make local and small-team data work fast, not to replace a distributed warehouse. |
| **Open source** | Dual-licensed MIT OR Apache-2.0. Yours to use, fork, and extend. |

---

## Status

Duckle is in **early development**. The visual designer, the DuckDB execution engine, scheduling, and cloud sources work today and are covered by integration tests. The surface area is still moving fast, the connector catalog is growing, and APIs may change. Treat it as a promising daily-driver-in-progress, not a 1.0.

**Scope, stated plainly:** Duckle is a single-machine, embedded studio. If you outgrow one box, point Duckle's output at the system that scales. It will not pretend to be a cluster.

The component palette ships **307 nodes** so the roadmap is visible in the product itself. As of the latest engine cut: **277 available**, **10 preview**, **20 planned**. Each node is tagged by availability:

- **Available** runs on the DuckDB engine today.
- **Preview** is configurable in the designer now (drag, wire, set properties); execution is being wired engine-by-engine. This currently covers the AI transforms and some vector DB read sources.
- **Planned** is on the roadmap and reserved in the palette, not yet executable. See [docs/roadmap.md](docs/roadmap.md) for what's left and why.

The capability matrix below marks each area accordingly.

---

## Screenshots

<p align="center">
  <img src="docs/assets/real-life-screenshot/1.png" alt="The Duckle visual designer with a CSV to Filter to Parquet pipeline" width="100%"/>
  <br/>
  <sub>Build a pipeline on the canvas, configure a node, and read the generated SQL. Here a CSV source flows through a Filter into a Parquet sink.</sub>
</p>

<p align="center">
  <img src="docs/assets/real-life-screenshot/2.png" alt="Component palette and schema autodetect" width="49%"/>
  <img src="docs/assets/real-life-screenshot/3.png" alt="Parquet sink configuration in dark theme" width="49%"/>
</p>
<p align="center">
  <sub>Left: the component palette and one-click schema autodetect from the source. Right: sink configuration with write mode, compression, and partitioning, in dark theme.</sub>
</p>

---

## Capabilities

Duckle is not a CSV tool with extras. It reads a broad set of formats and sources, ships a deep transform library, and writes to files, databases, object storage, and AI stores. CSV is just one source among many.

### Sources

| Group | Connectors | Status |
|---|---|---|
| **Files** | CSV, TSV, Parquet, JSON, JSONL / NDJSON, Excel (.xlsx), **YAML**, **TOML**, **Fixed-width** (mainframe / banking positional dumps) | Available |
| **Geospatial files** | GeoJSON, Shapefile, GeoPackage, KML, GPX, GML via the `spatial` extension | Available (lazy-loaded) |
| **Lakehouse table formats** | Apache Iceberg, Delta Lake, DuckLake | Available |
| **Embedded databases** | SQLite (read tables), DuckDB (read tables or run a query) | Available |
| **Network relational DBs** | PostgreSQL, MySQL, MariaDB, CockroachDB | Available (live CI tests for PG + MySQL) |
| **Network relational DBs** | **SQL Server** (TDS), **Oracle** (Instant Client at runtime), **ClickHouse** (HTTP API) | Available |
| **Network relational DBs** | IBM DB2, generic JDBC | Planned |
| **Object storage** | Amazon S3, Google Cloud Storage, Azure Blob, HTTP(S), MinIO, Cloudflare R2, Backblaze B2 | Available (live CI test for MinIO) |
| **Cloud warehouses** | MotherDuck (DuckDB-native) | Available |
| **Cloud warehouses** | **Snowflake** (SQL API + PAT/JWT auth, paginated), **BigQuery** (community extension), **Redshift** (postgres ATTACH), **Databricks SQL** (Statement Execution API + chunk follow), **Azure Synapse** (TDS) | Available |
| **Avro files** | Apache Avro container files (.avro / .ocf) via the pure-Rust `apache-avro` crate. The OCF header carries the schema; no schema config needed. | Available |
| **XML** | Read XML via `quick-xml` with a slash-separated rowPath; attributes prefix `@`, text goes to `_text`, repeated siblings collapse to arrays | Available |
| **Streaming** | **Apache Kafka / Redpanda** (batch-consume via the pure-Rust `rskafka` driver), **NATS JetStream** (subscribe-with-timeout via `async-nats`), **GCP Pub/Sub** (pull via REST API + auto-ack), **RabbitMQ** (basic_get poll loop via the pure-Rust `lapin` AMQP driver) | Available |
| **Streaming** | Pulsar, Kinesis, Event Hubs | Planned |
| **APIs and SaaS** | **REST** (cursor / offset / page / Link header pagination), **GraphQL**. Vendor tiles: **Salesforce, HubSpot, Pipedrive, Zendesk, Intercom, Stripe, QuickBooks, Xero, Shopify, Notion, Airtable, Asana, Trello, ClickUp, Monday.com, GitHub, GitLab, Linear, Jira, Mailchimp, SendGrid, Segment** (thin pre-configured wrappers over REST/GraphQL) | Available |
| **APIs and SaaS** | **OData v4** (thin alias over src.rest; default responsePath `/value` + follows `@odata.nextLink` across pages; works with SAP, D365, Microsoft Graph) | Available |
| **APIs and SaaS** | **SOAP** / **generic XML APIs** (thin alias; POST + `text/xml` defaults; `responseFormat=xml` walks the element path into the response body. SOAPAction header settable via `soapAction` prop. Namespace prefixes (`soap:Envelope`) match local-name in the row path) | Available |
| **APIs and SaaS** | gRPC, Google Sheets, Excel Online, webhook listener, more SaaS vendors | Planned |
| **NoSQL and search** | **MongoDB** (official driver), **Cassandra / ScyllaDB** (CQL), **Elasticsearch / OpenSearch** (from+size + search_after), **Redis** (SCAN + GET via the sync client), **CouchDB** (via the `_all_docs` REST endpoint) | Available |
| **NoSQL and search** | DynamoDB | Planned |
| **Repos and engineering data** | **Git** (commit log or file tree from a local working copy; shells out to the system `git` CLI - no Rust dep) | Available |
| **File transfer** | **FTP** (list + download via the pure-Rust `suppaftp` client; glob filter; each file becomes one row with base64-encoded content). SFTP is a separate protocol and a separate component. | Available |
| **Desktop** | **Clipboard** (read system clipboard via pure-Rust `arboard`; auto-detects JSON-array shape and unfolds it into rows). Desktop-only by design - fails clearly on headless systems. | Available |
| **Vector / AI databases** | **pgvector** (postgres ATTACH; server needs `CREATE EXTENSION vector`), **Qdrant** (`/collections/{id}/points/scroll` with cursor pagination), **Weaviate** (`/v1/objects?class=&after=` with cursor pagination), **Milvus** (`/v1/vector/query` with offset pagination) | Available |
| **Vector / AI databases** | Pinecone (no list-all-vectors API), Chroma, LanceDB | Preview |

### Transforms

50+ transforms compile to SQL and run today. All of the following are **available**:

| Group | Operations |
|---|---|
| **Fields** | Map (visual row mapper), Project / Select, Cast / Convert Type, Rename, Add Column, Drop Columns, Reorder, Coalesce / Null Fill, **UUID** (fresh UUID v4 per row - surrogate row id) |
| **Rows** | Filter (visual builder or raw SQL, with a **reject** port), Distinct, Sample, Top N / Limit, Sort, Skip / Offset, **Top N per Group** (row_number window + filter; ascending or descending), **Forward Fill** (replace NULLs with the last non-null value within an ordered window - time-series gap fill) |
| **Aggregate** | Group By, Rollup, Cube, Count Rows, **Window Aggregate** (SUM / AVG / COUNT / MIN / MAX OVER a window, keeps every row), **Cumulative** (running SUM / AVG / COUNT / MIN / MAX over an ordered window, per-group optional), **Approx Quantile** (median / p95 / p99 via t-digest, fixed memory regardless of cardinality), **Approx Count Distinct** (HyperLogLog, available as a function in the Group By dropdown) |
| **Join** | Inner, Left, Right, Full Outer, Cross, Lookup, Semi, Anti, **Spatial Join** (two-input join whose predicate is ST_Intersects / Contains / Within / Touches / Crosses / Overlaps / Equals; INNER or LEFT) |
| **Set operations** | Union, Union All, Intersect, Except / Minus |
| **Window** | Row Number, Rank, Dense Rank, Lead, Lag, First Value, Last Value, NTile |
| **Strings** | Regex Replace, **Regex Extract** (pull a capture group out of a column via `regexp_extract`), **Regex Match** (boolean column from `regexp_matches`), Split, Concat, Trim, Case Change, Length, Substring, Format, **Hash** (md5 / sha1 / sha256 for anonymization or deterministic IDs), **IP Parse** (extract host / family / netmask / broadcast / mask length / network from IP or CIDR text via the `inet` extension), **URL Parse** (extract scheme / host / port / path / query / fragment from URL columns), **Text Similarity** (pairwise score between two columns via `levenshtein` / `damerau_levenshtein` / `jaccard` / `jaro_winkler_similarity`), **Base64** (encode/decode mode), **Pad String** (left or right pad to a fixed length - zero-pad IDs, right-pad for fixed-width output), **Text Match** (boolean contains / starts_with / ends_with) |
| **Date / Time** | Parse, Format, Extract Part (year/quarter/month/week/day/hour/minute/second/**dayofweek**/**isodow**/**dayofyear**/**epoch**), Date Diff, Date Add, Truncate, Timezone Convert, **Time Bin** (round timestamps down to fixed-interval buckets - 5 minutes, 1 hour, 1 day, etc. - for time-series grouping), **Current Timestamp** (add pipeline-run time as a `loaded_at` / `processed_at` column), **Epoch Convert** (TIMESTAMP <-> Unix epoch seconds, both directions) |
| **Numeric** | Round, Modulo, Absolute, Logarithm, Power, Square Root, **Bucketize** (bin into N equal-width buckets between low and high), **Z-Score** (per-row standardized value computed against the whole input via window aggregates), **Clamp** (clip values to a [low, high] range - cap outliers before stats), **Sign** (-1 / 0 / +1) |
| **JSON / nested** | Parse JSON, Stringify, Flatten, JSONPath Extract, Merge Objects, **Array Aggregate** (collapse rows into a JSON array per group via `json_group_array`) |
| **Array** | Explode / Unnest, Collect List, Element At, Contains, Array Distinct, **Array Length** (scalar count of list elements) |
| **Pivot / shape** | Pivot, **Unpivot**, **Denormalize** (group + delimited cells), **Normalize** (explode delimited / array column), **Transpose** |
| **CDC / SCD** | **Diff Detect** (tag inserted / updated / deleted rows vs a previous snapshot), **SCD Type 1**, **SCD Type 2** (versioned history with valid_from / valid_to / is_current), **Merge / Upsert** |
| **AI / Search** | **Vector Similarity Search** (cosine / L2 / inner product over FLOAT[N] embeddings via `vss`, optional top-K), **Full-Text Search** (BM25 over chosen columns via `fts`, optional top-K), **Embeddings** (per-row vector via any OpenAI-compatible `/v1/embeddings` endpoint; works with OpenAI, Cohere, Voyage, llama.cpp embed server via `baseUrl`; `apiKey` lives in the stage's props for now) |
| **Geospatial** | **Spatial Distance** (ST_Distance to a target WKT geometry), **Spatial Buffer** (ST_Buffer around each row's geometry), **Spatial Intersects** (boolean: does each row overlap a target geometry, via ST_Intersects - pair with Filter Rows to keep only matches) - lazy-load the spatial extension on first use |
| **Debug** | Log Rows (pass through and print to Output for mid-pipeline inspection), **Assert** (hard-fail the pipeline if any row violates a SQL predicate - defensive ETL check, complements qa.* row-level validators that route to a reject port) |

The Embeddings transform ships with the apiKey-in-props credential pattern. The other 5 AI transforms (LLM Transform, Text Chunker, PII Redact, Classify, Semantic Dedupe) stay in **preview** until they're wired through the same pattern - the engine plumbing is the same shape (per-row API call, batched).

### Data quality

The whole group runs today. Validators split their input: passing rows continue on the main port, failures route to a **reject** port you can sink, count, or inspect.

| Component | Behavior | Status |
|---|---|---|
| **Not-Null Check** | Pass rows with no nulls in the chosen columns | Available |
| **Range Check** | Pass rows inside a numeric range (inclusive or exclusive) | Available |
| **Regex Match** | Pass rows whose column fully matches a pattern | Available |
| **Uniqueness Check** | Pass the first row per key; route duplicates to reject | Available |
| **Schema Validate** | Reject rows where any expected column is null | Available |
| **Column Profile** | Per-column stats (count, null %, distinct, min / max, quartiles) via `SUMMARIZE` | Available |
| **Describe** | Column names + types of the input | Available |
| **Histogram** | Value frequencies for one column, most-frequent first | Available |
| **Standardize** | Trim + case-normalize + collapse inner whitespace, in place | Available |
| **Fuzzy Deduplicate** | Keep the first row per near-duplicate cluster (Jaro-Winkler / Levenshtein) | Available |
| **Record Match** | Self-join: emit pairs of rows above a similarity threshold, with a match score | Available |
| **Address Cleanse** | Address parsing / normalization | Planned (needs external library) |

### Custom code and reusable SQL

| Capability | What it does | Status |
|---|---|---|
| **Inline SQL** | Write a `SELECT`; the upstream node is exposed as `input`, and the result runs as a real materialized stage | Available |
| **SQL Template** | Parameterized SQL with `${context.var}` substitution | Available |
| **SQL routines** | Reusable, named SQL saved in the workspace and executable inside any pipeline | Available |
| **Shell** | Run any shell command; one output row with `{stdout, stderr, exit_code, duration_ms}`. Platform-aware default shell (cmd.exe on Windows, /bin/sh on Unix). Optional `timeoutMs` kills the child; cancellation does the same | Available |
| **WebAssembly UDF** | Per-row WASM transform via pure-Rust `wasmi` interpreter. Sandboxed (no fs/net/env). Module supplies a `transform(i32, i32) -> i64` export; engine writes the input column into module memory, calls transform, reads result back. Works with any WASM toolchain (Rust, AssemblyScript, C, Tinygo) | Available |
| **Python / Rust / JavaScript UDFs** | Embedded-language stages | Planned |

### Sinks

| Group | Connectors | Status |
|---|---|---|
| **Files** | CSV, TSV, Parquet (ZSTD), JSON, JSONL / NDJSON, Excel (.xlsx), **YAML**, **TOML**, **XML** (configurable root + row element wrappers), **Avro** (schema inferred from first row, or supply JSON schema) - Parquet and CSV support Hive-partitioned writes via `PARTITION_BY (col1, col2)` with `OVERWRITE_OR_IGNORE` semantics | Available |
| **Geospatial files** | GeoJSON, GeoPackage, Shapefile, KML, GPX via GDAL | Available (lazy-loaded) |
| **Lakehouse table formats** | Apache Iceberg (full table layout), DuckLake | Available |
| **Embedded databases** | SQLite, DuckDB (write a table) | Available |
| **Network relational DBs** | PostgreSQL, MySQL, MariaDB, CockroachDB - write modes: **overwrite**, **append**, **truncate**, **upsert** (ON CONFLICT / ON DUPLICATE KEY UPDATE via passthrough) | Available (live CI for PG + MySQL) |
| **Network relational DBs** | **SQL Server / Azure Synapse** (TDS, multi-row VALUES batched at 1000), **Oracle** (built-in, Instant Client at runtime; INSERT ALL idiom), **ClickHouse** (HTTP JSONEachRow) | Available |
| **Network relational DBs** | IBM DB2, generic JDBC | Planned |
| **Object storage** | Amazon S3, Google Cloud Storage, Azure Blob via DuckDB `httpfs` (MinIO / R2 / B2 via endpoint) | Available |
| **Cloud warehouses** | MotherDuck | Available |
| **Cloud warehouses** | **Snowflake** (SQL API; PAT or JWT RS256 auth), **BigQuery** (community extension), **Redshift** (postgres ATTACH; all PG write modes), **Databricks SQL** (Statement Execution API + PAT), **Azure Synapse** (TDS) | Available |
| **HTTP APIs** | **REST** (POST/PUT/PATCH a single batched JSON-array request) and **Webhook** (one POST per row). Bearer / API-key auth + custom headers via the form. Uses `ureq` blocking client. **GraphQL** sink for mutations. | Available |
| **NoSQL** | **MongoDB** (official driver, insert_many batched), **Cassandra / ScyllaDB** (CQL prepared INSERT), **Elasticsearch / OpenSearch** (`_bulk` NDJSON), **Redis** (pipelined SET with optional EXPIRE) | Available |
| **NoSQL** | DynamoDB | Planned |
| **Streaming** | **Apache Kafka / Redpanda** (batch-produce via the pure-Rust `rskafka` driver), **NATS JetStream** (publish via `async-nats`; optional per-row subject suffix), **GCP Pub/Sub** (publish via REST API; OAuth2 Bearer auth), **RabbitMQ** (persistent-delivery publish via the pure-Rust `lapin` AMQP driver; exchange + routing key) | Available |
| **Streaming** | Pulsar, Kinesis | Planned |
| **SOAP** | (use REST sink at the SOAP endpoint until a dedicated component lands) | Planned |
| **Vector / AI databases** | **pgvector** (Postgres ATTACH), **Pinecone** (`/vectors/upsert`), **Qdrant** (`/collections/{id}/points` PUT), **Weaviate** (`/v1/batch/objects`), **Milvus** (`/v1/vector/insert`) | Available |
| **Vector / AI databases** | Chroma, LanceDB | Preview (need a vendor SDK to land dedicated handlers) |

### Control flow

| Component | What it does | Status |
|---|---|---|
| **Replicate / Tee** | Send the same data to multiple downstream outputs | Available |
| **Merge Streams** | Concatenate multiple input streams (UNION ALL) | Available |
| **Switch / Conditional Split** | Route rows to `case_1..N` outputs by boolean condition (first match wins); rows that don't match any condition fall to a `default` output | Available |
| **Wait / Delay** | Sleep `N ms / s / min / h` before passing rows through. Useful for rate-limiting a downstream API or stretching out smoke tests. | Available |
| **Throttle** | Insert an inter-stage delay derived from a rows-per-second target. Best-effort for batch pipelines, the hook is in place for streaming. | Available |
| **Checkpoint** | Pass rows through and also write a parquet snapshot to a path. The durable artifact a future run can read back via `src.parquet`. | Available |
| **Dead Letter Queue** | Terminal sink for rejected rows (JSON / CSV / Parquet at a path). Conventionally wired to an upstream node's reject port. | Available |
| **Run Pipeline** | Inline-execute another pipeline file (`ctl.runpipeline`) - reusable sub-pipelines | Available |
| **Iterate** | Run a sub-pipeline N times with `${ITER_INDEX}` substitution (`ctl.iterate`) | Available |
| **For Each** | Run a sub-pipeline once per input row with `${ITER_ITEM_<FIELD>}` substitution (`ctl.foreach`) | Available |
| **Try / Catch** | Install a fallback sub-pipeline that runs only if the wrapped stage fails (`ctl.try`) | Available |
| **Retry** | Per-stage retry policy: configure on any stage's Advanced tab (retry_attempts + retry_backoff_ms); the `ctl.retry` tile is a visual marker for that policy | Available |
| **Schedule** | Cron / interval / file-watch triggers via the orchestration crate (configured in the Schedule panel rather than as a graph node) | Available |
| **Trigger** | External trigger sources (webhook listener, message bus tap) | Planned (paired with the streaming-sources work) |

### Advanced settings (per-node)

Every node has an **Advanced** tab in the Properties panel with fields the engine honours at run time:

| Field | What it does |
|---|---|
| **Retry attempts** | Total tries on failure (1 = no retry). The engine sleeps `backoff * attempt` ms between attempts. |
| **Retry backoff (ms)** | Inter-attempt sleep, linearly scaled by attempt index. |
| **Memory limit (MB)** | `PRAGMA memory_limit` applied to this stage only - cap a heavy aggregation without touching the whole pipeline. |
| **Log row count** | Print the post-stage rowcount to the run output. |

### Orchestration and workspace

| Capability | What it does |
|---|---|
| **Run feedback** | Streaming run events light nodes up stage by stage, with per-node row counts, a real mid-query cancel, and run history. |
| **Schedules** | Cron, fixed-interval, and file-watch triggers, driven by an in-process scheduler. |
| **Context variables** | Per-environment variables; bind any field to one via a Manual / Context dropdown, or reference `${var}` inline. Resolved at run time. |
| **Cloud credentials** | Saved S3 / GCS / Azure connections become DuckDB SECRETs; cloud reads and writes go through `httpfs`. S3-compatible endpoints (MinIO / R2 / B2) supported via `ENDPOINT` + `URL_STYLE`. |
| **Workspace** | Pipelines, connections, contexts, documents, and routines persist per-pipeline as plain JSON and Markdown files in a folder you choose. |

---

## Clean data before it reaches your AI

Models inherit the quality of their inputs. RAG indexes, embedding stores, and training sets quietly accumulate duplicates, nulls, malformed rows, mixed encodings, and inconsistent schemas. Duckle is built to scrub that data before it lands in a vector store:

- **Deduplicate** with exact Distinct, Uniqueness, and **Fuzzy Deduplicate** (Jaro-Winkler / Levenshtein); use **Record Match** to find near-duplicate pairs with a similarity score.
- **Profile + describe** every column up front (Column Profile, Describe, Histogram) so issues surface before they reach a model.
- **Validate and filter** malformed, empty, or out-of-range records and route failures to a reject port.
- **Normalize** types, encodings, casing, and null handling across messy sources (Standardize, Cast, regex / string transforms).
- **Retrieve with both halves of hybrid search**, locally, no model API required: **Vector Similarity Search** (cosine / L2 / inner product over FLOAT[N] embeddings) and **Full-Text Search** (BM25 over chosen columns). Top-K supported on both.
- **Land it in your store** - relational sinks (PostgreSQL, MySQL, MariaDB, CockroachDB) write with `upsert` (ON CONFLICT) so vector tables stay idempotent. **pgvector** ships, and the major vector DBs (**Pinecone**, **Qdrant**, **Weaviate**, **Milvus**) all have working sinks that POST batches through each vendor's HTTP API.

> The AI transforms that still need a model API (Embeddings, LLM Transform, Text Chunker, PII Redact, Classify, Semantic Dedupe) are **preview**: you can drag, wire, and configure them now (provider, collection, embedding column, distance metric). Their execution is landing engine-by-engine. The retrieval pair (Vector Similarity Search + Full-Text Search) is **available today** through DuckDB's `vss` and `fts` extensions - no API key required.

---

## Engines

Duckle ships a thin shell and installs its engine on first launch, which is why the download stays tiny.

| Engine | Role | Status |
|---|---|---|
| **DuckDB** | Default execution engine: analytics, file formats, cloud reads, SQL pushdown. Tracking **v1.5.3** (latest stable). | Working |
| **SlothDB** | Alternate embedded analytical engine ([SouravRoy-ETL/slothdb](https://github.com/SouravRoy-ETL/slothdb)), installed the same way and selectable per pipeline. | Installable |
| **Native** | In-process Rust streaming / incremental engine. | Planned |

DuckDB is the default. **SlothDB is a drop-in alternate engine**: install it from the same guided first-run screen and switch to it from the engine selector in the toolbar, with no change to your pipeline. Both downloadable engines install with a progress bar and no manual setup.

### First-launch extension pre-fetch

When the first-launch installer downloads the DuckDB CLI it also pre-fetches the extensions Duckle uses, with per-extension progress in the install modal, so the first time you touch a Postgres source or an Iceberg table there is no surprise network hop mid-pipeline:

`httpfs` (S3 / GCS / HTTP), `azure` (Azure Blob native), `sqlite`, `postgres`, `mysql`, `excel`, `iceberg`, `delta`, `ducklake`, `vss`, `fts`.

`spatial` is lazy-loaded (~50 MB GDAL bundle) - it installs on first use of a geospatial source/sink to keep the initial download small. `avro` stays preview until the community extension publishes a v1.4+ build.

---

## Quickstart (60 seconds)

1. **Download** the latest release for your OS, or build from source below.
   - Windows: `Duckle_x64-setup.exe` (installer) or the standalone `duckle.exe`.
2. **Launch it.** On first run, Duckle offers to install its engine. Click **Install DuckDB** (a small download with a progress bar).
3. **Pick a workspace folder.** This is where your pipelines and config live as plain files.
4. **Build a pipeline:**
   - Drag a **CSV source** in, point it at [`samples/orders.csv`](samples/orders.csv), and hit **Autodetect schema**.
   - Drag a **Filter**, wire it up, and add a condition like `status = 'paid'`.
   - Drag a **Parquet sink** and choose an output path.
   - Press **Run**, watch the nodes light up, then open the **Output** tab.

That is a real, native ETL pipeline, built and run in under a minute. CSV is just the easiest first node; swap in Parquet, JSON, SQLite, DuckDB, or an S3 URL the same way.

---

## How to use Duckle

1. **Sources** - drag a source onto the canvas and point it at a file, an embedded database, or a cloud URL. Click **Autodetect schema** to read the columns and a sample.
2. **Transforms** - drag transforms and wire them to the source's output port. Configure each in the properties panel; the **Preview** tab shows live rows and the **Plan** tab shows the generated SQL.
3. **Data quality** - drop in a validator (Not-Null, Range, Regex, Uniqueness). Passing rows continue on the main port; failures leave the **reject** port, which you can sink or inspect separately.
4. **Sinks** - finish with a sink (file, SQLite, DuckDB, or cloud) and set its path and write mode.
5. **Run** - press **Run** to execute on DuckDB (or SlothDB). Nodes light up stage by stage; the **Output** and **Console** tabs report row counts, timing, and errors.
6. **Reuse** - save Connections, Context variables, and SQL Routines in the workspace; reference `${context.var}` in any field. Everything persists as plain files you can commit.
7. **Schedule** - attach a cron, interval, or file-watch trigger to run a pipeline automatically.

---

## Documentation

Duckle documents itself as you build, and the reference lives in this repo:

- **In-app** - every node has inline field help, a live **Preview** tab, and a **Plan** tab that shows the exact generated SQL.
- **Component reference** - the [Capabilities matrix](#capabilities) lists every source, transform, validator, and sink with its current status.
- **Quickstart and how-to** - the [60-second quickstart](#quickstart-60-seconds) and [How to use Duckle](#how-to-use-duckle) above.
- **Build and contribute** - [Build from source](#build-from-source) and [CONTRIBUTING](CONTRIBUTING.md).
- **Samples** - ready-to-run example data under [`samples/`](samples).

A hosted documentation site is on the roadmap.

---

## Build from source

**Prerequisites**

- [Rust](https://rustup.rs/) (stable)
- [Node.js](https://nodejs.org/) 18+ and npm
- [`cargo-tauri`](https://tauri.app/) CLI: `cargo install tauri-cli --version "^2"`
- Platform webview dependencies per the [Tauri prerequisites](https://tauri.app/start/prerequisites/). WebView2 is preinstalled on Windows 10 and 11.

**Clone and install**

```bash
git clone https://github.com/SouravRoy-ETL/duckle
cd duckle
npm --prefix frontend install
```

**Run in development** (hot-reloading frontend plus the native shell):

```bash
cargo tauri dev
```

**Build a release binary and installers:**

```bash
cargo tauri build
```

Outputs land in `target/release/` (the standalone `duckle.exe`) and `target/release/bundle/` (the `.msi` and NSIS `-setup.exe` installers). The engine is not compiled in: DuckDB downloads at first launch, which is why the build is fast and the binary is tiny.

**Run the tests:**

```bash
cargo test                      # unit and plan/compile tests, no engine needed
# end-to-end tests drive a real DuckDB CLI:
DUCKLE_DUCKDB_BIN=/path/to/duckdb cargo test
```

---

## Architecture

```
duckle/
  apps/desktop/         Tauri 2 shell: commands, engine installer, window
  frontend/             React 19 + Vite + TypeScript: the designer UI
  crates/
    duckdb-engine/      Compiles the node graph to SQL and drives the DuckDB CLI
    slothdb-engine/     SlothDB adapter
    scheduler/          Cron / interval / file-watch triggers
    metadata/           Schema and type model
    plugin-sdk/         Connector / inspector traits
    connectors/         Source and sink connectors
    runtime, workflow-engine, transform-engine, stream-engine, execution-core
```

- The **frontend** (React with [@xyflow/react](https://reactflow.dev/)) is the visual designer; it talks to the Rust core over Tauri commands.
- **duckdb-engine** topologically sorts the graph, lowers each node into SQL, and executes by shelling out to the downloaded DuckDB CLI. Non-sink nodes materialize as tables so later stages can reference them; sinks become `COPY ... TO` statements; cancel kills the process. No statically linked database, so the binary stays small.
- **Everything persists** to the workspace folder you choose, as plain JSON and Markdown files.

---

## Roadmap

A complete planned-component breakdown lives in [`docs/roadmap.md`](docs/roadmap.md). At a glance:

- [ ] **Streaming connectors** (Kafka, Pulsar, NATS, Kinesis, Event Hubs, Pub/Sub, RabbitMQ, Redpanda) - need the broker drivers; the engine hook (continuous-pipeline mode) lands alongside
- [ ] **Vector-DB read endpoints** (Pinecone, Qdrant, Weaviate, Chroma, Milvus, LanceDB) - each vendor's scan API is bespoke; writes already ship via the HTTP sinks
- [ ] **OAuth-heavy SaaS** (Google Sheets, Excel Online, full Salesforce, Xero, QuickBooks OAuth flow) - the simple-auth REST tiles ship today; full OAuth is on the roadmap
- [ ] **Custom-code stages** (Python, JavaScript, Rust, Wasm, Shell UDFs) - sandboxing and embed-engine choices are open scope decisions
- [ ] **AI transforms with model API** (Embeddings, LLM Transform, Text Chunker, PII Redact, Classify, Semantic Dedupe) - need a provider-credential pattern; the search half (Vector Similarity Search + Full-Text Search) ships now
- [ ] **In-process Native engine** - a Rust streaming / incremental executor as an alternative to shelling out to the DuckDB CLI
- [ ] **Plugin marketplace** via the connector SDK
- [ ] Hosted documentation site

---

## Contributing

Contributions, issues, and ideas are welcome. Duckle is young and there is a lot of green field. Open an issue to discuss a change before a large PR, match the existing code style, and keep changes focused. Run `cargo test` and `npm --prefix frontend run build` before submitting.

---

## License

Licensed under either of **MIT** or **Apache-2.0** at your option.

---

<div align="center">
<sub>Built with Rust, Tauri, React, and DuckDB by <a href="https://github.com/SouravRoy-ETL">Sourav Roy</a></sub>
</div>

<!-- Suggested GitHub topics: etl, elt, data-engineering, data-pipeline, duckdb, rust, tauri, react, typescript, local-first, embedded, drag-and-drop, data-cleaning, vector-database, ai, desktop-app -->
