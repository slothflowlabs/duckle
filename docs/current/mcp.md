# MCP server (connect any LLM to Duckle)

`duckle-mcp` is a [Model Context Protocol](https://modelcontextprotocol.io)
server. It lets any MCP client - Claude Desktop, Claude Code, or any other LLM
agent that speaks MCP - drive Duckle directly: browse the component catalog,
generate a pipeline straight into a folder you choose, validate it, run it, read
existing pipelines and their run logs, build a standalone artifact, and manage
saved connections.

It speaks newline-delimited JSON-RPC over stdio and reuses the DuckDB engine
in-process, so there is no GUI and no Node runtime involved at run time.

---

## 1. Build it

```bash
cargo build -p duckle-mcp --release
# binary at target/release/duckle-mcp (.exe on Windows)
```

The server has no network dependencies and embeds the full component catalog at
build time. To run and build pipelines it needs two binaries it locates at run
time:

- **DuckDB CLI** - for `run_pipeline` and `build_pipeline`. Point `DUCKLE_DUCKDB_BIN`
  at it (or pass `duckdb` per call, or have `duckdb` on PATH).
- **duckle-runner** - for `build_pipeline` only. Point `DUCKLE_RUNNER_BIN` at a
  `duckle-runner` binary (or have it on PATH / next to `duckle-mcp`). Use a
  release-profile runner so built artifacts stay small.

---

## 2. Connect a client

**Easiest:** in Duckle, click the **MCP** button (robot icon) in the designer
top bar. The popup bundles the server, fills in the real paths, and offers a
one-click **Connect to Claude Code** plus copy buttons for the command and the
mcpServers config. The steps below are the manual equivalent (or for a server
built from source).

### Claude Code

```bash
claude mcp add duckle -- /path/to/duckle-mcp
```

Set the engine paths in the environment Claude Code launches the server with, or
inline:

```bash
claude mcp add duckle --env DUCKLE_DUCKDB_BIN=/path/to/duckdb --env DUCKLE_RUNNER_BIN=/path/to/duckle-runner -- /path/to/duckle-mcp
```

### Claude Desktop (and other clients using the standard config)

Add to the client's `mcpServers` config:

```json
{
  "mcpServers": {
    "duckle": {
      "command": "/path/to/duckle-mcp",
      "env": {
        "DUCKLE_DUCKDB_BIN": "/path/to/duckdb",
        "DUCKLE_RUNNER_BIN": "/path/to/duckle-runner"
      }
    }
  }
}
```

---

## 3. Tools

| Tool | What it does |
|---|---|
| `list_components` | List components, optionally filtered by `kind` (source/transform/sink/control/quality/custom) or a `query` substring. |
| `get_component_schema` | Full property schema (form fields + input/output ports) for one `componentId`. |
| `create_pipeline` | Validate a pipeline and write `<name>.json` into a chosen `directory`. Fails without writing if it does not compile (unless `validate:false`). |
| `validate_pipeline` | Compile a pipeline to SQL without running it. Returns per-stage SQL or a structured error. |
| `run_pipeline` | Run a pipeline headlessly. Returns per-node status, row counts, errors and a small result preview. |
| `list_pipelines` | List pipeline `.json` files in a directory with node/edge counts. |
| `read_pipeline` | Read and return a pipeline file. |
| `read_run_logs` | Read the tail of a pipeline's NDJSON run log (component-level events). |
| `backfill_list` | List the saved state a pipeline resumes from - watermarks, snapshot ids, Kafka offsets, spool positions, tumbling-window buffers. Each says whether it can be set by hand. |
| `backfill_set` | Set a watermark or snapshot id. Refused when the node holds a different kind of state. |
| `backfill_clear` | Remove a node's state so it starts over. Not always a full reload - see the note below. |
| `build_pipeline` | Build a pipeline into ONE self-contained executable for server deployment (see [scheduler.md](scheduler.md)). |
| `list_connections` | List the workspace's saved connections (secret fields masked). |
| `create_connection` | Create a saved connection JSON so pipelines can reference its fields. |

Also exposed: resources `duckle://catalog` (the full catalog) and
`duckle://pipeline-format`, and a `generate_pipeline` prompt.

---

## 4. Pipeline format

A pipeline is JSON with `name`, `nodes`, and `edges`:

```json
{
  "name": "orders to csv",
  "nodes": [
    { "id": "src", "type": "source", "position": {"x": 0, "y": 0},
      "data": { "label": "orders", "componentId": "src.csv",
                "properties": { "path": "orders.csv", "hasHeader": true } } },
    { "id": "snk", "type": "sink", "position": {"x": 300, "y": 0},
      "data": { "label": "out", "componentId": "snk.csv",
                "properties": { "path": "out.csv", "mode": "overwrite" } } }
  ],
  "edges": [
    { "id": "e1", "source": "src", "target": "snk",
      "sourceHandle": "main", "targetHandle": "main",
      "data": { "connectionType": "main" } }
  ]
}
```

Use `list_components` + `get_component_schema` to discover component ids and
their property keys. Transforms add ports beyond `main` (e.g. `reject`,
`lookup_1`, `case_1`, `main_1`); the edge `data.connectionType` mirrors the
handle.

### Credentials

Never inline real secrets. Put a `${ENV:KEY}` placeholder in a property and
provide the value through the environment at run time. The headless runner (and
any built artifact) resolves `${ENV:KEY}` from the process environment first,
then a `secrets.env` file, then an encrypted `secrets.enc`. See
[scheduler.md](scheduler.md) for the build-time secret modes.

---

## 5. Regenerating the catalog

The component catalog the server embeds is exported from the frontend manifest
(the single source of truth) into `crates/duckle-mcp/catalog.json`:

```bash
npm --prefix frontend run export-catalog
```

Re-run this whenever components or their property schemas change, then rebuild
the crate.
