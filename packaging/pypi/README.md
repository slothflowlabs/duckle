# Duckle

**ETL/ELT pipelines you own, on DuckDB. Author and deploy to your servers or cloud.** Python builds the plan, DuckDB moves the rows, with no vendor cloud in between.

[![PyPI](https://img.shields.io/pypi/v/duckle.svg)](https://pypi.org/project/duckle/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://github.com/slothflowlabs/duckle/blob/main/LICENSE)
[![GitHub](https://img.shields.io/github/stars/slothflowlabs/duckle?style=social)](https://github.com/slothflowlabs/duckle)

## Build your first pipeline

Paste this into Claude Code, Cursor, or Codex. The agent does the rest.

> Run `uvx duckle quickstart` to build my first pipeline and run it

Nothing to install first. The agent fetches Duckle and the DuckDB engine on demand, runs a real pipeline, and shows you the rows.

Prefer to drive it yourself:

```sh
uvx duckle quickstart      # no install
pip install duckle         # or install it
```

<img src="https://raw.githubusercontent.com/slothflowlabs/duckle/main/docs/assets/pypi-demo-install.svg" alt="Terminal: uvx duckle quickstart scaffolds sample data and a pipeline, runs it, and prints the resulting rows" width="660"/>

`quickstart` scaffolds sample data and a pipeline, runs it, and shows you the rows. One command from nothing to a result, because the engine comes with it: a ~20 MB native binary plus the DuckDB CLI. No JVM, no Docker, no server, no account.

---

## Or install it

<img src="https://raw.githubusercontent.com/slothflowlabs/duckle/main/docs/assets/pypi-demo-pip.svg" alt="Terminal: pip install duckle brings the DuckDB engine, then a job.py using the Python API reads a CSV, filters, derives a column and writes Parquet" width="660"/>

Where `uvx` is for trying it, `pip` is for keeping it: a persistent `duckle` command, `duckle-mcp` for agents, and `import duckle` in Python.

## No data passes through Python

Every method appends a node to a pipeline graph. Nothing executes until `.run()`, and then the whole graph is handed to the engine, compiled to SQL, and executed inside DuckDB.

That is the difference from a dataframe library: a billion-row job costs what the SQL costs, because it *is* SQL. There is no interpreter in the data path, and no `to_pandas()` escape hatch quietly pulling rows into memory.

```python
import duckle
from duckle import col

(duckle.read_csv("orders.csv")
    .where(col.amount >= 20)
    .derive(total="round(amount * 1.2, 2)", tag="f'{region}-{id}'")
    .write_parquet("out.parquet")
    .run())
```

## Python expressions, compiled to SQL

`amount * 1.2`, `f'{a}-{b}'`, `x if c else y`, `name.strip().upper()` and `email is None` are translated to vectorized DuckDB SQL **at plan time**:

```python
.derive(band="'high' if amount > 50 else 'low'")
# CASE WHEN ("amount" > 50) THEN 'high' ELSE 'low' END
```

An expression with no exact SQL equivalent is **rejected with the construct named**, never quietly executed somewhere slower. Comprehensions, `lambda`, indexing and `eval` are refused by name.

Prefer editor completion over strings? `col` builds the same tree with real operators, and fragments are reusable:

```python
eu_only = col.region.isin(["EU", "UK"])
p.where(eu_only & (col.amount >= 20))
```

See exactly what will run, before it runs:

```python
p.explain()   # prints the compiled SQL, one block per stage
```

## 421 components, not 10 file formats

Component ids map onto attribute paths, so anything in the catalog is reachable:

```python
duckle.src.salesforce(object="Account", authMode="clientCredentials")
duckle.xf.geo.reproject(geomColumn="geom", targetCrs="EPSG:3857")
duckle.snk.salesforce.bulk(operation="upsert", externalIdField="Ext__c")
```

125 sources, 77 sinks and 145 transforms: Postgres, MySQL, Oracle, SQL Server, Snowflake, Databricks, Teradata, SAP OData, Salesforce (including Bulk API 2.0), Kafka, WebSocket, S3, SFTP, IMAP, LanceDB and more.

```python
duckle.component_ids(contains="salesforce")
duckle.describe("snk.salesforce.bulk")   # settings, and which ones the engine ignores
```

## A CI gate that needs no engine

<img src="https://raw.githubusercontent.com/slothflowlabs/duckle/main/docs/assets/pypi-demo-validate.svg" alt="Terminal: duckle validate reports one failing and one passing pipeline, then exits 1" width="660"/>

`duckle validate` compiles every pipeline without opening a source or writing a sink, so it needs **no DuckDB, no credentials and no network**. Exit codes are stable and safe to gate on:

| code | meaning |
|---|---|
| `0` | clean |
| `1` | a real finding: a pipeline failed, or did not compile |
| `2` | the runner could not start: bad usage, missing engine |

```sh
duckle validate --json          # machine-readable, for a build step
duckle --pipeline my.json       # run one
```

Note: `validate` does not yet catch every missing required property, so a clean validate is not proof that a run will succeed.

## Agent-ready, with nothing installed

<img src="https://raw.githubusercontent.com/slothflowlabs/duckle/main/docs/assets/pypi-demo-agent.svg" alt="Terminal: adding duckle as an MCP server via uvx, then an agent discovering Salesforce connectors, reading a component schema, creating a pipeline that compiles, and running it" width="660"/>

The same package is an MCP server, so an AI agent gets a governed way to work with data instead of a shell. Point Claude Code, Claude Desktop or Cursor at it:

```json
{ "mcpServers": { "duckle": { "command": "uvx", "args": ["duckle", "mcp"] } } }
```

Or in Claude Code:

```sh
claude mcp add duckle -- uvx duckle mcp
```

That is the whole setup. No pip install, no PATH, no engine to configure: uv fetches the package and the DuckDB engine into a throwaway environment and the server finds it there. If you did `pip install duckle`, `"command": "duckle"`, `"args": ["mcp"]` works the same way.

**19 tools**, including `list_components`, `get_component_schema`, `create_pipeline`, `validate_pipeline`, `run_pipeline`, `pipeline_lineage`, `trust_report` and `schema_drift`.

What that buys over letting an agent write a script: it can discover a real connector rather than guess one, compile-check a pipeline **before anything executes**, run it, and get column-level lineage back. `validate_pipeline` opens no source and writes no sink, so an agent can check its work without touching your data. The pipeline it produces is the same JSON your desktop canvas opens, so you can inspect what it built.

## Code and canvas are the same file

Pipelines are the same JSON the [Duckle desktop studio](https://github.com/slothflowlabs/duckle) reads. A pipeline written in Python opens on the canvas, and one drawn on the canvas runs from Python:

```python
p.save("pipelines/orders.json")          # opens in the studio
duckle.from_json("pipelines/orders.json").run()
```

## Install notes

`duckle` depends on [`duckdb-cli`](https://pypi.org/project/duckdb-cli/), published by the DuckDB Foundation, so the engine arrives with the install and works offline. To pin your own build instead, set `DUCKLE_DUCKDB_BIN`.

Wheels ship for Linux, macOS and Windows on x86-64 and arm64. The wheel carries a compiled Rust binary and is Python-version independent (`py3-none-<platform>`).

---

Apache-2.0 &nbsp;·&nbsp; [GitHub](https://github.com/slothflowlabs/duckle) &nbsp;·&nbsp; [duckle.org](https://duckle.org) &nbsp;·&nbsp; [Issues](https://github.com/slothflowlabs/duckle/issues)
