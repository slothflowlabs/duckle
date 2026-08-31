<div align="center">

<img src="docs/assets/duckle-readme.png" alt="Duckle" width="460"/>

<h3>Pipelines you own. Author and deploy to your servers or cloud.</h3>

<p><b>Duckle</b> is an open-source ETL platform for teams who want their pipelines running on their own infrastructure. Author on a canvas, in Python or in SQL, then ship the same file to your own server or cloud account: <code>duckle-runner serve</code> runs it headless on a schedule, in Docker or on a box you own, with a web console, roles and an audit trail. Every pipeline is one file in git, so it outlives whoever wrote it. It compiles to SQL on DuckDB and uses every core you give the box, so a bigger instance is a faster pipeline: <b>96 million rows out of Postgres to Parquet in 39.9s</b>. <b>No vendor cloud. No per-row billing. No lock-in.</b></p>

<a href="https://duckle.org/"><img src="website/assets/img/website-hero.gif" alt="Duckle connecting 190 sources and destinations - databases, warehouses, SaaS apps and the DuckDB ecosystem - all running locally on DuckDB" width="600"/></a>

<p><sub><i>Duckle is an independent open-source project by SlothFlowLabs. It builds on the DuckDB engine but is not part of, affiliated with, or endorsed by DuckDB Labs or MotherDuck.</i></sub></p>

<p>
<img alt="status" src="https://img.shields.io/badge/status-beta-3b82f6?style=for-the-badge"/>
<a href="https://github.com/slothflowlabs/duckle/releases"><img alt="downloads" src="https://img.shields.io/github/downloads/slothflowlabs/duckle/total?style=for-the-badge&amp;logo=github&amp;logoColor=white&amp;label=DOWNLOADS&amp;color=2b6cb0"/></a>
<img alt="clones" src="https://img.shields.io/endpoint?url=https%3A%2F%2Fraw.githubusercontent.com%2Fslothflowlabs%2Fduckle%2Fmain%2F.github%2Fbadges%2Fclones.json&amp;style=for-the-badge&amp;logo=github&amp;logoColor=white"/>
<img alt="stars" src="https://img.shields.io/github/stars/slothflowlabs/duckle?style=for-the-badge&amp;logo=github&amp;logoColor=white&amp;label=STARS&amp;color=f59e0b"/>
<a href="https://discord.com/invite/rUeAStJbWb"><img alt="discord" src="https://img.shields.io/discord/1498599942246109265?style=for-the-badge&amp;logo=discord&amp;logoColor=white&amp;label=DISCORD&amp;color=5865F2"/></a>
<br/>
<img alt="license" src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue?style=for-the-badge"/>
<img alt="platforms" src="https://img.shields.io/badge/platforms-Windows%20%C2%B7%20macOS%20%C2%B7%20Linux-2b6cb0?style=for-the-badge"/>
<img alt="duckdb" src="https://img.shields.io/badge/DuckDB-FFF000?style=for-the-badge&amp;logo=duckdb&amp;logoColor=black"/>
</p>

</div>

<div align="center">

<a href="https://trendshift.io/repositories/54176?utm_source=trendshift-badge&amp;utm_medium=badge&amp;utm_campaign=badge-trendshift-54176" target="_blank" rel="noopener noreferrer"><img src="https://trendshift.io/api/badge/trendshift/repositories/54176/daily?language=Rust" alt="slothflowlabs%2Fduckle | Trendshift" width="250" height="55"/></a>

<a href="https://discord.com/invite/rUeAStJbWb"><img src="docs/assets/discord-cta-v2.svg" alt="Join the Duckle community on Discord" width="340"/></a>

<p><sub><a href="https://github.com/slothflowlabs/duckle/stargazers"><b>Star Duckle</b></a> if it looks useful. It genuinely helps other data engineers find the project.</sub></p>

</div>

---

## Quick links

<table>
<tr>
<td valign="top" width="25%">

**Get started**

- [Where Duckle runs](#where-duckle-runs)
- [What is Duckle?](#what-is-duckle)
- [What's new in v0.7.1](#whats-new-in-v071)
- [What's new in v0.7.0](#whats-new-in-v070)
- [What's new in v0.6.1](#whats-new-in-v061)
- [What's new in v0.6.0](#whats-new-in-v060)
- [Quickstart (60 s)](#quickstart-60-seconds)
- [Download / Install](#download--install)
- [Build from source](#build-from-source)
- [Run your first pipeline](#run-your-first-pipeline)

</td>
<td valign="top" width="25%">

**Use the product**

- [Meet Duckie (AI)](#meet-duckie---the-local-ai-pipeline-assistant)
- [How to use Duckle](#how-to-use-duckle)
- [Recipes / examples](#recipes-and-examples)
- [In-app Git (GitHub/GitLab)](#git-integration-github--gitlab)
- [Workspace + Git flow](#workspace-and-git-flow)
- [Schedules](#schedules-and-triggers)
- [Plans](#plans-several-pipelines-in-an-order-you-chose)
- [Server deployment](#server-deployment-build-pipeline)
- [Sign-in and roles](#sign-in-and-roles)
- [How a request is decided](#how-a-request-is-decided)
- [API keys, for machines](#api-keys-for-machines)
- [MCP server: connect Claude, Cursor or any agent](#mcp-server-connect-claude-or-any-llm-to-duckle)
- [Connection management](#connection-management)
- [Context variables](#context-variables)

</td>
<td valign="top" width="25%">

**Reference**

- [Capabilities matrix](#capabilities)
- [Sources](#sources)
- [Transforms](#transforms)
- [Sinks](#sinks)
- [Data quality](#data-quality)
- [Custom code](#custom-code)
- [Control flow](#control-flow)
- [Advanced settings](#advanced-settings-per-node)
- [Engines](#engines)
- [Configuration](#configuration)

</td>
<td valign="top" width="25%">

**Resources**

- [Architecture](#architecture)
- [Clean data for AI](#clean-data-before-it-reaches-your-ai)
- [Performance tips](#performance-tips)
- [FAQ](#faq)
- [Troubleshooting](#troubleshooting)
- [CI / CD](#ci--cd)
- [Status](#status)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [Sponsor Duckle](SPONSORS.md)
- [License](#license)
- [Releases](https://github.com/slothflowlabs/duckle/releases)
- [Roadmap doc](docs/roadmap.md)
- [Contributing doc](CONTRIBUTING.md)

</td>
</tr>
</table>

---

## What is Duckle?

An open-source ETL platform you run on your own infrastructure. Drag sources, transforms, validators and sinks onto a canvas, wire them together, and press **Run**. Duckle compiles the graph to SQL and executes it on a real columnar engine, with live previews, the generated SQL visible on every node, and no hidden state.

You build a pipeline on a laptop and deploy that same file to a server, where it runs on a schedule under a web console with roles, alerts and an audit log. Nothing is rewritten in between, and nothing is metered.

In short: a free, open-source, single-engine alternative to hosted, per-row-priced ETL platforms like Fivetran and Airbyte - one pipeline for ingest, transform, and load that runs anywhere, and can also run dbt on DuckDB inside the same tool.

Three things set it apart:

1. **An AI assistant that ships in the box.** Describe the pipeline you want in English; Duckie writes the JSON and drops it onto the canvas. The model runs wherever Duckle does - no API key, no telemetry, no vendor round-trip. Point it at your own OpenAI-compatible endpoint instead if you would rather it did not run in-process.
2. **360+ components ready at install time.** Files, lakehouses, SQL databases, warehouses, NoSQL, vector DBs, streaming brokers, SaaS REST/GraphQL APIs, even FTP and IMAP - working today, not coming-soon.
3. **A self-contained binary you can audit.** 73 to 110 MB depending on your platform. Engines install on first launch. Workspaces are plain files in a folder you choose. Diff them, branch them, ship them.

<div align="center">
<img src="docs/assets/flow.svg" alt="Sources flow through 50+ transforms into files, databases, object storage, vector stores, and AI" width="100%"/>
</div>

---

## Why Duckle is different

| | |
|---|---|
| **Visual, never opaque** | The canvas compiles to SQL you can read, and every node has a live preview tab. No black box. |
| **An assistant with no API key** | Runs in-process by default, or against your own OpenAI-compatible endpoint. Your prompts and your data stay inside your infrastructure either way. |
| **Single-file binary, no bundled DB** | 73 to 110 MB depending on platform (it embeds the headless runner + MCP server). DuckDB downloads on first launch with a guided step. AI engine is opt-in. |
| **Native speed** | Execution runs through DuckDB: vectorized, columnar, local. A clean-and-export job that crawls in a spreadsheet finishes in milliseconds. |
| **Git-friendly by design** | Pipelines, connections, contexts, and routines persist as plain files in a folder you pick. Diff them, branch them, review them. |
| **360+ components ready today** | Files, databases, warehouses, lakehouses, object stores, SaaS APIs, NoSQL, streaming brokers, vector DBs, FTP, IMAP, SMTP. Each is covered by tests. |
| **Honest about scope** | Single-machine and embedded by design. Built to make local and small-team data work fast, not to replace a distributed warehouse. |
| **60 UI languages** | Topbar, palette, chat assistant, properties panel, and common dialogs ship localized. English, Spanish, Chinese (Simplified + Traditional), Hindi, Arabic, Portuguese (Brazil), Bengali, Russian, Japanese, Punjabi, German, Korean, French, Vietnamese, Telugu, Marathi, Turkish, Tamil, Urdu, Persian, Polish, Italian, Ukrainian, Indonesian, Thai, Dutch, Hebrew, Swedish, Greek, Czech, Hungarian, Romanian, Filipino, Malay, Norwegian, Danish, Finnish, Catalan, Bulgarian, Slovak, Croatian, Serbian, Slovenian, Lithuanian, Latvian, Estonian, Khmer, Burmese, Sinhala, Nepali, Swahili, Afrikaans, Welsh, Irish, Icelandic, Albanian, Azerbaijani, Mongolian, Kazakh. RTL (Arabic, Hebrew, Persian, Urdu) supported. Switch languages from the topbar globe. |
| **Open source** | Dual-licensed MIT OR Apache-2.0. Yours to use, fork, and extend. |

---

## Screenshots

Real pipelines, built and run in Duckle - not mockups.

<p align="center">
  <img src="docs/assets/real-life-screenshot/mega-pipeline-join.png" alt="A 5-million-row pipeline joining a CSV, a Parquet file, a DuckDB table, and a SQLite table through the visual Map node" width="100%"/>
  <br/>
  <sub>A 5M-row pipeline: a CSV, a Parquet file, a DuckDB table, and a SQLite table enriched through one visual <b>Map</b> (3-way join), no SQL.</sub>
</p>

<p align="center">
  <img src="docs/assets/real-life-screenshot/visual-mapper.png" alt="The visual Map editor showing a main input, two lookups, per-output expressions, and an inline filter" width="49%"/>
  <img src="docs/assets/real-life-screenshot/parallelize-canvas.png" alt="A Parallelize node fanning out aggregate, window, and top-N branches across the canvas" width="49%"/>
</p>
<p align="center">
  <sub>Left: the visual <b>Map</b> editor - main plus lookups, per-output expressions, an inline filter. Right: <b>Parallelize</b> fanning out aggregate, window, and top-N branches.</sub>
</p>

<p align="center">
  <img src="docs/assets/real-life-screenshot/mega-pipeline-parallelize.png" alt="A run summary showing 16 nodes finishing in roughly three seconds across parallel branches writing to Parquet, CSV, DuckDB, and SQLite" width="100%"/>
  <br/>
  <sub>One run, many branches: 16 nodes finish in a few seconds. Concurrency auto-detects from CPU cores; branches write to Parquet, CSV, DuckDB, and SQLite at once.</sub>
</p>

<p align="center">
  <img src="docs/assets/real-life-screenshot/cdc-ducklake.png" alt="A DuckLake CDC change-feed pipeline mirroring 100k changes into a DuckDB table with upsert and delete propagation" width="49%"/>
  <img src="docs/assets/real-life-screenshot/incremental-load.png" alt="A watermark incremental load reading 5 million rows and appending only new rows" width="49%"/>
</p>
<p align="center">
  <sub>Left: <b>DuckLake CDC</b> change-feed mirrored via <b>upsert + delete propagation</b> (100k rows). Right: <b>watermark incremental load</b> over 5M rows, advancing state only on a fully successful run.</sub>
</p>

---

## Quickstart (60 seconds)

1. **Download** the binary for your OS (see [Download / Install](#download--install) above) - or [build from source](#build-from-source).
2. **Launch it.** First run shows the setup modal:
   - Click **Install** on DuckDB (required, takes ~30 s).
   - Optionally click **Install** on Duckie AI Assistant (~1.1 GB, takes 5-10 min on average broadband).
3. **Pick a workspace folder.** Pipelines, connections, context variables, and routines live there as plain files.
4. **Build a pipeline two ways:**
   - **Drag + wire**: drag a **CSV source** in, point it at [`samples/orders.csv`](samples/orders.csv), hit **Autodetect schema**. Drag a **Filter**, wire it up. Drag a **Parquet sink** with an output path. Press **Run**, watch the nodes light up.
   - **Ask Duckie**: click the **Sparkles** icon (top-right of the toolbar), type *"read orders.csv, filter where status = 'paid', write to paid.parquet"*. When Duckie streams back a pipeline, click **Insert into canvas**.
5. **Inspect.** Click any node to see its generated SQL in the **Plan** tab and a live row sample in the **Preview** tab.

That's a real, native ETL pipeline built and run in under a minute. CSV is just the easiest first node; swap in Parquet, JSON, S3, Snowflake, MongoDB, or Stripe the same way.

---

## Download / Install

Pick the binary for your OS from the [latest release](https://github.com/slothflowlabs/duckle/releases/tag/v0.7.1):

| OS | Asset | How to run |
|---|---|---|
| **Windows** | `Duckle-windows-x64.exe` | Double-click. Unsigned binary - Windows SmartScreen will warn the first time; click "More info" -> "Run anyway". |
| **macOS** (Apple Silicon) | `Duckle-macos-arm64` | `chmod +x Duckle-macos-arm64 && ./Duckle-macos-arm64`. Right-click -> Open the first time to bypass Gatekeeper. |
| **Linux** (x86_64) | `Duckle-linux-x64` | `chmod +x Duckle-linux-x64 && ./Duckle-linux-x64`. Requires WebKitGTK 4.1 (`libwebkit2gtk-4.1-0` on Debian / Ubuntu). |

The single-file binary above is all you need for **Build Pipeline** too: the headless runner is embedded into the app at build time, and exporting a pipeline produces ONE self-contained executable (the engine, the DuckDB CLI, any needed extensions, and the resolved pipeline are all inside that one file). Copy that single file to your server and run or schedule it - no separate runner download required.

<p align="center"><img src="docs/assets/pypi-demo-install.svg" alt="Terminal: uvx duckle quickstart scaffolds sample data and a pipeline, runs it, and prints the resulting rows" width="660"/></p>

One command, nothing installed: it scaffolds sample data and a pipeline, compiles it to SQL, runs it on DuckDB, and shows you the rows.

```sh
uvx duckle quickstart
```

### Let an agent do it

Paste this into Claude Code, Cursor, or Codex:

> Run `uvx duckle quickstart` to build my first pipeline and run it

Nothing to install first. The agent fetches Duckle and the DuckDB engine on demand, runs a real pipeline, and shows you the rows.

### CLI only (CI, cron, containers)

If you do not want the desktop studio, install just the headless runner. It is about 27 MB rather than 100 MB or more, has no GUI dependency, and is what a build step actually needs.

```sh
pip install duckle
```

That is the whole install. It brings the DuckDB CLI with it (via the [`duckdb-cli`](https://pypi.org/project/duckdb-cli/) package published by the DuckDB Foundation), so there is nothing else to fetch and it works offline. Wheels ship for Linux, macOS and Windows on x86-64 and arm64.

<p align="center"><img src="docs/assets/pypi-demo-pip.svg" alt="Terminal: pip install duckle brings the DuckDB engine, then a job.py using the Python API reads a CSV, filters, derives a column and writes Parquet" width="660"/></p>

It also gives you a Python API, where pipelines are built as code and executed by DuckDB rather than by Python:

```python
import duckle
from duckle import col

(duckle.read_csv("orders.csv")
    .where(col.amount >= 20)
    .derive(total="round(amount * 1.2, 2)")
    .write_parquet("out.parquet")
    .run())
```

Python expressions compile to vectorized SQL at plan time, so no rows pass through the interpreter. See [the PyPI page](https://pypi.org/project/duckle/) for the full API.

The same package provides the `duckle` command-line runner for CI, cron, and containers - it bundles the headless runner and the MCP server per platform:

```sh
pip install duckle          # or run ad hoc, no install: uvx duckle --help
```

Pipelines execute as SQL on the DuckDB CLI, so the runner needs a `duckdb` on PATH or `DUCKLE_DUCKDB_BIN` set (`pip install duckdb-cli` is the quickest route). Validation does not:

```sh
duckle validate                 # compile-check every pipeline under ./pipelines
duckle validate --json          # machine-readable, for a CI step
duckle --pipeline my.json       # run one
```

`validate` opens no source and writes no sink, so it needs no engine, no credentials and no network. Exit codes are stable: `0` clean, `1` a real finding (a pipeline failed or did not compile), `2` the runner could not start (bad usage, unreadable file, missing engine).

The binary is 73 to 110 MB depending on platform (it embeds the headless runner and the bundled MCP server). On first launch you'll be guided through downloading two engines into your app-data directory:

| Engine | Size | Required? | What it powers |
|---|---|---|---|
| **DuckDB CLI** | ~30 MB + extensions | **Yes** - cannot run pipelines without it | Every source / transform / sink that runs as SQL |
| **Duckie AI Assistant** | ~1.1 GB (llama-server + Qwen 2.5 Coder 1.5B GGUF) | Optional | The chat sidebar that generates pipelines from natural language |

App-data location:
- Windows: `%APPDATA%\io.duckle.app\engines\`
- macOS: `~/Library/Application Support/io.duckle.app/engines/`
- Linux: `~/.config/io.duckle.app/engines/`

Delete the `engines/` folder if you ever want to force a fresh install.

---

## Run your first pipeline

A worked example using the bundled `samples/orders.csv` data.

### 1. Add a source

- Open the **Components** sidebar (left). Click **Sources -> Files -> CSV**.
- Drag it onto the canvas.
- In the right-side Properties panel:
  - **Path**: browse to `samples/orders.csv`
  - Click **Autodetect schema** - the **Schema** tab fills in column types from the file, the **Preview** tab shows the first 20 rows.

### 2. Add a transform

- **Components -> Transforms -> Rows -> Filter**. Drag onto canvas.
- Wire the CSV source's `main` output port to the Filter's `main` input.
- In Properties:
  - **Predicate**: `status = 'paid'` (you can write raw SQL or use the visual builder)
  - Filter has two output ports: `pass` (rows matching) and `reject` (rows that don't).

### 3. Add a sink

- **Components -> Sinks -> Files -> Parquet**.
- Wire Filter's `pass` port to the Parquet sink.
- **Path**: `paid_orders.parquet`. **Write mode**: `overwrite`. **Compression**: `zstd`.

### 4. Run it

- Press **Run** in the toolbar. Nodes light up in execution order; row counts appear under each.
- Open the **Output** tab (bottom panel) to see per-stage timing.
- Click any node to inspect generated SQL in **Plan** + sampled rows in **Preview**.

### 5. Iterate

- Add a **Group By** before the sink to aggregate. Re-run. Sub-second on small data.
- Cancel mid-run with the **Stop** button - the DuckDB process is killed cleanly.
- Save your work: **Cmd/Ctrl-S** writes a JSON pipeline file to your workspace folder.

---

## Where Duckle runs

You build a pipeline on your laptop. The server runs that same file. Nothing is rewritten, exported or converted in between.

```mermaid
flowchart LR
    D["Duckle Desktop<br/>your machine"] -->|deploy, needs admin| W
    B["Console in a browser<br/>your machine"] -->|turn it on, needs operator| W
    W["Workspace on your server<br/>a new schedule lands OFF"] --> C["Scheduler<br/>every 15s, takes what is due"]
    C --> R["It runs<br/>on that box, unattended"]
    R --> O["Run history, logs, metrics,<br/>alerts, and an audit log"]
    O -->|you watch it here| B
```


| | How | What you get |
|---|---|---|
| **Server** | `duckle-runner serve --workspace /srv/pipelines` | Headless web console, cron scheduler, roles, audit log, alerts |
| **Docker** | `Dockerfile.web` | The same console in a container, behind your own ingress |
| **CI** | `duckle-runner --pipeline p.json` | Any runner. Exit codes and NDJSON logs, nothing to install |
| **Standalone** | **Build Pipeline** | One self-contained executable. Drop it on a box, run it from cron or systemd |
| **Desktop** | The app | Author, debug and inspect. Optional, and never required to run anything |

Nothing here depends on a person's machine being switched on:

- **Pipelines are plain files in git.** Review them in a pull request, roll them back, and let them outlive whoever wrote them. There is no proprietary repository and no exported binary artifact.
- **The console has roles and an audit log,** so more than one person can operate it and you can see who did what.
- **Secrets are not in the pipeline file.** They resolve from the environment or an encrypted per-workspace store at run time.

Working recipes for **AWS (EC2, ECS, EKS)**, **Azure (VM, Container Apps, AKS)** and **Google Cloud (Compute Engine, GKE)**, with manifests and the mistakes worth avoiding, are at **[duckle.org/deploy](https://duckle.org/deploy.html)**. Three things worth knowing before you start:

- On a non-loopback bind with no credential the console starts **unclaimed**, and for 15 minutes anyone who can reach it can claim it and become its administrator. Pass `--token`, set `DUCKLE_CONSOLE_TOKEN`, or create accounts with `duckle-runner console add-user` before exposing it. An empty value is refused rather than treated as absent, so an unresolved secret fails loudly instead of opening that window. Who can do what, and [how one request is decided](#how-a-request-is-decided), is set out under [Sign-in and roles](#sign-in-and-roles).
- The **scheduler runs in `serve`**, not in the editor. Start the editor with schedules armed and it now says so rather than leaving you to wonder why nothing fired.
- **`GET /healthz`** needs no credential and answers `ok`, so a Kubernetes probe or a load balancer can check liveness without holding a token. Every other route is authenticated, so pointing a probe anywhere else reports the pod unhealthy forever.

### Deploying a pipeline to a running server

`POST /api/deploy` lands a pipeline on a server from wherever it was authored, with the schedule it should eventually run on:

```bash
curl -X POST https://duckle.internal/api/deploy \
  -H "Authorization: Bearer $DUCKLE_TOKEN" \
  -d '{"name":"orders-load",
       "pipeline": '"$(cat orders-load.json)"',
       "schedule":{"intervalMinutes":30}}'
```

Two things are deliberate. **The schedule arrives disabled**, so a cadence someone set while testing on a laptop cannot start firing the moment it reaches production; enabling it is a separate call. And **deploying needs `admin` while enabling needs `operator`**, because a deployed pipeline runs shell and SQL on that host: shipping the code and starting it are two acts, and the audit log records both with the name of whoever did them.

### API keys, for machines

A person signs in and gets a session. A machine has no browser and nobody to rotate a password, so it gets a key of its own:

```bash
duckle-runner console key-add ci-deployer --role admin --expires-days 90
duckle-runner console key-list      # role, state, and when each was last used
duckle-runner console key-revoke ci-deployer
```

A key carries **its own role**, so a deploy runner can be `admin` while a metrics scraper is `viewer`. It is printed once and stored only as a hash, so a lost key is replaced rather than recovered. `key-list` shows when each was last used, which is the question actually worth answering before revoking one, and **revoking takes effect immediately on a console that is already running** rather than at the next restart. Revoked keys are marked rather than deleted, so a key that turns up in an old log can still be named.

Accounts, sessions and keys live in `.duckle/console.db`. An existing `console-users.json` is carried into it on first start and renamed to `.migrated`, so an upgrade neither locks anyone out nor destroys the only copy of a credential store. For the roles, and a diagram of [how one request is decided](#how-a-request-is-decided), see [Sign-in and roles](#sign-in-and-roles).

### Scaling it

`duckle-runner serve` is an ordinary service. Run it on EC2, EKS, a VM or a container next to everything else you operate, and scale it the way you scale any service:

- **More cores.** The engine is parallel and uses every core on the box by default, so a bigger instance is a faster pipeline with no change to the pipeline. Bound it with `DUCKLE_THREADS` when you would rather it did not take the whole machine.
- **More RAM.** Set `memoryLimitMb` per stage, or a workspace default, and spill to disk past it.
- **More pipelines at once.** `DUCKLE_MAX_CONCURRENT_RUNS` raises how many run together; it ships at 1 so an unattended server stays predictable until you decide otherwise.
- **More machines.** `duckle-runner work` drains a queued batch from as many workers as you start, on as many hosts as you like, each claiming its items under a lock so nothing runs twice.
- **Bigger than any box.** Turn on **pushdown** and the query runs verbatim inside Postgres, Oracle, SQL Server or Snowflake. The warehouse does the scan; Duckle keeps the scheduling, lineage, data quality and alerting.

Measured, rather than asserted:

- **96,000,000 rows** out of live Postgres to Parquet in **39.9s** ([details](#96m-rows-postgres-to-parquet))
- **Oracle extract at 65.0s**, against 68.6s for python-oracledb with pyarrow on the same machine ([details](#whats-new-in-v061))

The one thing Duckle does not do is split a single query across a cluster the way a distributed warehouse does. When you need that, push the work down into the system that has it and let Duckle orchestrate around it.

## Server deployment (Build Pipeline)

> Want the studio to publish straight to a running server instead? That is the other
> route: connect a server once, then **Deploy to a server** from the editor. Step by step
> in [docs/current/server-deployment.md](docs/current/server-deployment.md).
>
> Promoting from CI instead, or driving Duckle from Airflow, Dagster or Temporal? See
> [docs/current/ci-and-orchestration.md](docs/current/ci-and-orchestration.md), with
> copyable GitHub Actions and GitLab CI templates in [docs/ci/](docs/ci/).
>
> Want to know exactly what crosses the wire, and where every credential is stored?
> [docs/current/client-server-architecture.md](docs/current/client-server-architecture.md)
> is the diagrammed answer, sharp edges included.

The in-app scheduler runs only while Duckle is open. To run a pipeline on a server with no desktop app, **Build Pipeline** turns it into ONE self-contained executable - the equivalent of a standalone "Job".

Right-click a pipeline (in the project tree or on the canvas) and choose **Build Pipeline**. The output is a single file named after the pipeline (`orders_etl.exe` on Windows, `orders_etl` on macOS / Linux) that embeds everything it needs:

- the headless execution engine,
- the DuckDB CLI,
- only the DuckDB extensions that pipeline's components actually use,
- the resolved pipeline (context variables substituted, routines inlined),
- its secrets (see below).

On first run it self-extracts to a temp cache and uses its **own** embedded DuckDB, so the server needs nothing installed - no Duckle, no DuckDB. There is no folder to copy, no `run.sh`, and no separate runner download. A CSV-to-CSV pipeline builds to about 28 MB; only the extensions a pipeline uses are bundled, so the file stays lean.

```bash
./orders_etl            # or orders_etl.exe on Windows
```

The process exits `0` on success and non-zero on failure, and writes the same NDJSON run logs under `logs/` (Splunk / Dynatrace friendly).

**Build options**

| Option | What it does |
|---|---|
| **Target OS** | Pick **Windows**, **Linux**, or **macOS** in the build dialog. The native OS always builds; a **Linux** server file can be cross-built from any host (the Linux engine is bundled for you), while a macOS file can only be produced on a Mac. Appending the payload makes the file unsigned, so do not codesign / Authenticode-sign it. |
| **Context** | Pick a context at build time; its non-secret variables are baked into the pipeline. |
| **Secrets: Environment** | Each secret becomes a `${ENV:KEY}` placeholder, so nothing sensitive is written into the file. The runner resolves real environment variables first, then a `secrets.env` (KEY=VALUE lines) placed next to the file. |
| **Secrets: Passphrase** | Secrets are encrypted inside the file with AES-256-GCM, decrypted at run time from the `DUCKLE_BUNDLE_PASSPHRASE` environment variable. |

**Schedule it** with whatever the server already has - point the OS scheduler straight at the file:

```cron
# Linux cron - run every day at 02:00
0 2 * * * /opt/duckle/orders_etl >> /var/log/orders_etl.log 2>&1
```

On Windows use **Task Scheduler**; on macOS a **launchd** plist; on Linux a **systemd** timer. Full examples in [docs/current/scheduler.md](docs/current/scheduler.md).

**Run against an existing workspace** - the same embedded headless runner can also execute a pipeline JSON directly, resolving context the way the app does:

```bash
duckle-runner --pipeline /path/to/pipeline.json [--workspace /path/to/workspace] [--duckdb /path/to/duckdb]
```

### Continuous mode (`follow`)

A scheduled pipeline already consumes a stream without gaps: a source that
tracks its position (`src.kafka` with `trackOffset`, `xf.incremental`) resumes
where the last **successful** run stopped. What a schedule cannot give is
latency - the scheduler wakes every 15 seconds, and every run pays process
start, DuckDB resolution and document parsing again.

`follow` keeps the same execution model and removes that per-batch overhead.
The document is read and resolved once, the engine is built once, and the
pipeline then runs in a loop. Each pass is one micro-batch:

```bash
duckle-runner follow /path/to/pipeline.json --idle-ms 500
```

| Flag | Meaning |
|---|---|
| `--idle-ms N` | wait N ms after a pass whose sinks wrote nothing (default 1000) |
| `--max-batches N` | stop after N passes (default: until stopped) |
| `--on-error stop\|continue` | stop on a failed batch (default), or keep going |

**A failed batch never advances the source position.** The position is queued
during the run and written only when the run reaches `ok`, which is after every
sink has written - so a failure anywhere, transform, quality gate or sink,
leaves the position where it was and the next pass re-reads exactly the records
that did not land. Killing the process is safe for the same reason; `Ctrl-C`
finishes the batch in hand first, which only saves you a truncated output file.

That ordering is the difference between a correct micro-batch loop and a lossy
one, so it is covered by a regression test that fails if the position ever
advances past a batch that did not land.

### Masking what you look at, not what you write

A pipeline's sinks can be perfectly governed and its data still be read off a
screen. Previews, profiles, reject rows, error bodies, API responses and the
rows an MCP tool hands an agent are all places production person-data appears
without anyone's permissions being wrong.

Tag the column in the schema:

```json
{ "name": "email",      "type": "string", "tags": ["pii"] }
{ "name": "api_secret", "type": "string", "tags": ["secret"] }
```

| tag | what an inspection surface shows |
|---|---|
| `secret` | `***`, always, whatever else the column is tagged |
| `pii` | a short stable digest, so rows stay distinguishable without the value appearing |
| `mask:null` / `mask:last4` / `mask:redact` / `mask:hash` | picked explicitly |

**It never changes what the pipeline writes.** The sink still receives the real
value, because the sink is governed by policy and the screen is not. Changing
written data is what `qa.mask` is for.

Masking happens as the previews are assembled, so the desktop panel, the CLI,
the console API and MCP are consistent by construction rather than by four
callers remembering. Both execution paths are covered, and there is a test for
each - a masking point wired into only one of them would leak on the other.

Nothing is inferred from a column name. A heuristic that masked `company_name`
because it contains "name" would teach people to distrust the masking, and one
that quietly failed to mask something would be worse.

### Schedules in a named time zone

A cron expression is civil time: `0 3 * * *` means three in the morning as a
person reads a clock. With no zone set that is the machine's clock, which is
what every existing schedule already means. Set one and it stops depending on
where the runner is deployed:

```yaml
timezone: Europe/Brussels
```

A Brussels registry pipeline stays at 03:00 Brussels whether the container runs
on UTC or the operator is watching from another continent. An unknown zone is
**refused when you save it**, not at fire time, so `Europe/Brussel` is a typo
you see rather than a job quietly running on UTC for a quarter.

**Daylight saving is decided, not discovered.** Twice a year a civil time is not
a single instant, so both cases are pinned and tested:

| case | what happens |
|---|---|
| the clock skips it (spring) | the occurrence is **skipped**, and the skip is reported. A job asked to run at 02:30 on a day with no 02:30 has not been missed by the scheduler; the day was short. It is not nudged to 03:30. |
| the clock repeats it (autumn) | it fires **once, at the earlier** of the two instants |

**Intervals are untouched.** `every 24 hours` is an elapsed duration, not "the
same clock time tomorrow", and a zone must not quietly turn one into the other.

Both schedulers - the desktop one and the web console's - now evaluate through
the same code. They disagreed once before, and the way it showed up was one
expression firing at two different times depending on which surface owned it.

**Days a schedule must not fire on** are a small calendar rather than a holiday
provider:

```yaml
exclude:
  weekdays: [sunday]
  dates: [2026-12-25]
```

The dates are civil dates **in the schedule's own zone**, which is why this
belongs with time zones rather than beside them: a schedule at 00:30 Brussels on
the 25th is 23:30 UTC on the 24th, so a UTC-based check would exclude the wrong
day. A skipped occurrence is reported rather than merely not happening, and a
misspelled weekday or date is refused when you save it - "sundy" excludes
nothing, which looks exactly like no exclusion at all until the day arrives.

Real holiday calendars vary by country, region and year; a first version that
tried to know them would be wrong somewhere and confidently so. A date list is
something an operator can check by reading it.

### Bounding what a workspace accumulates (`retention`)

A long-running server grows run history, run logs, receipts and a stage cache,
and nothing the pipelines do bounds any of it:

```bash
duckle-runner retention status --json
duckle-runner retention prune --cache-days 45 --logs-days 30 --receipts-keep 50 --dry-run
duckle-runner retention prune --cache-days 45 --logs-days 30 --receipts-keep 50
```

```text
category          files        bytes  oldest
cache               412   1204338112  61d
logs                 38      1102944  90d
runs                  6        49122  12d
receipts             57        27991  12d
```

**Retention is opt-in per category.** A bare `prune` with no limits removes
nothing, because housekeeping that deletes by default is how a workspace loses
something nobody meant to lose.

**`.duckle/` is never touched, at all.** It holds the workspace encryption key,
saved watermarks and resume positions, known host keys and the accepted XSD
contracts. Losing a watermark does not lose history - it silently re-ingests or
skips data, which is a correctness problem rather than a housekeeping one. The
check runs when the plan is built **and again when it is applied**, so that
"never touches state" is a property of the code rather than of the caller.

The **audit log is never pruned by age**: it is the record of the prune's own
deletions, and every prune appends to it.

`--dry-run` and a real prune call the same planning function and differ only in
whether the deletion runs, so the two cannot disagree about what would go.

### Reports CI already understands (`--format`)

`validate` emits the shapes CI systems and agents actually consume, so nothing
has to scrape console text:

```bash
duckle-runner validate --format json    # versioned envelope
duckle-runner validate --format junit   # every CI renders this as a test report
duckle-runner validate --format sarif   # GitHub Code Scanning, and most editors
```

SARIF puts each finding on the file it is about, with forward-slash URIs so Code
Scanning can match them to repository paths. JUnit keeps the passing checks as
well as the failures, because a report with two failures and no passes cannot be
told from one where only two things ran.

**The exit codes are part of the contract:**

| code | meaning |
|---|---|
| `0` | everything checked passed |
| `1` | a check failed - the thing being checked is wrong |
| `2` | the tool could not run - bad flag, unreadable file, no input |

`1` and `2` are deliberately different. A job usually wants to fail differently
on `2`, because `2` means the gate never actually ran, and treating that as a
pass is how a broken gate goes unnoticed for a month.

`--json` is unchanged and is the same document as `--format json`: the versioned
envelope carries the old `results` array alongside the new `findings`, so an
existing consumer keeps working and a new one gets `schemaVersion`.

### Retry a failed run (`retry`)

Every run writes a small receipt under `<workspace>/runs/receipts/` and prints
its id. `retry` takes that id and says what repeating the run would do, before
doing any of it:

```bash
duckle-runner retry run-daily-1788171319319 --dry-run
duckle-runner retry run-daily-1788171319319 --rerun-sinks
duckle-runner retry run-daily-1788171319319 --json          # for CI and agents
```

```text
  run    extract                  it failed last time
  reuse  parse                    <ws>/cache/daily/parse/9f2c....parquet
  WRITE  publish                  a sink writes outside the run
```

**It refuses more than it reuses, on purpose.** A retry stops before planning
anything when the pipeline has changed since that run, when the engine version
has, when the run being retried actually succeeded, or when it would write again
to a sink. Nothing in the engine can tell a sink that is safe to repeat from one
that is not, so that decision is not made for you: `--rerun-sinks` is how you
say you have checked. `--allow-changed` retries a changed pipeline with reuse
switched off, because the recorded outputs describe work that no longer exists.

**Reuse is verified, not assumed.** A node is only reused when its recorded
output is still on disk, checked by looking. A receipt saying a node succeeded
is not evidence that its output survived, and a cache pruned since is the normal
way it does not.

**What it does not do yet.** There is no `--from <node>`: compiling a downstream
subgraph does not exist, and a flag that always errors is worse than no flag.
Reuse only ever covers nodes with `cacheOutput` set, which is opt-in and offered
by six components, so a pipeline that never ticked the box reuses nothing and
the plan says so on every line. And only runs started by `duckle-runner
--pipeline` write a receipt today, so a run from the API, the scheduler or the
desktop app answers `retry:no-receipt` rather than guessing.

### Backfill without the desktop app (`backfill`)

Production deployments are headless, so replaying from an earlier point should
not mean getting at the server's workspace through a GUI:

```bash
duckle-runner backfill list  --pipeline ./pipelines/daily.json
duckle-runner backfill set   --pipeline ./pipelines/daily.json --node inc --value 2026-01-01 --type TIMESTAMP
duckle-runner backfill clear --pipeline ./pipelines/daily.json --node inc
duckle-runner backfill list  --pipeline ./pipelines/daily.json --json      # for CI and agents
```

**Five node kinds keep state in that folder, and only two resume from a value a
person can write down.** `xf.incremental` (a watermark) and
`src.ducklake.changes` (a snapshot id) can be set; a `src.kafka` resume offset,
a `src.spool` byte position and an `xf.tumble` buffer pointer are listed and can
be cleared, but `set` on them is **refused**. Writing `{value,type}` over a
tumbling window's state would drop the pointer to the rows it is holding and
delete them on the next run, with nothing to report it.

Clearing is not always a full reload, and the tool says so: a Kafka node with
`startFrom: latest` skips whatever is already in the topic when it has no saved
offset, so clearing it moves PAST that backlog rather than replaying it.

The same three operations are on the console API and MCP, so a replay can be
driven from CI or an agent as well as the CLI:

```
GET    /api/watermarks?file=pipelines/daily.json          viewer
POST   /api/watermarks?file=pipelines/daily.json          operator
DELETE /api/watermarks?file=pipelines/daily.json&node=ID  operator
```

Reading needs a viewer; changing what the next run processes needs an
operator. All four surfaces - desktop panel, CLI, API, MCP - call the same
engine functions, so the kind guard cannot be bypassed by picking a different
one.

### Push sources that do not lose what arrives (`listen` + `src.spool`)

`src.webhook` and `src.websocket` collect INSIDE a pipeline run: they bind or
connect, take N messages or time out, and stop. Right for a one-shot capture,
wrong for anything continuous - between runs the port is closed and arriving
requests are refused. Under `follow` that gap is every batch boundary.

`listen` is the other half. It keeps the listener up and appends what arrives
to an append-only NDJSON spool; a pipeline reads that spool with `src.spool`,
from wherever the last **successful** run stopped:

```bash
duckle-runner listen --port 9000 --spool ./spool/hooks.ndjson --path-filter /hooks
duckle-runner follow ./pipelines/hooks.json --idle-ms 500
```

Arrival is decoupled from processing, so a slow batch, a failed batch or a
restart costs nothing that already arrived. Append-only plus a byte offset is
the whole trick: the reader never deletes and the writer never rewrites, so
there is no race between them.

A record is `{received_at, method, path, headers, json|body}` - a JSON body is
embedded under `json` so the pipeline can address its fields, and anything else
is kept verbatim under `body` rather than dropped for not parsing. The spool is
written and flushed BEFORE the 200 goes out, because a 200 tells the sender its
delivery is safe and webhook senders do not retry those.

### Resource budget (`--memory-limit`, `--threads`, `--max-temp-size`)

Duckle targets one machine and will use it. On a dedicated box that is what
you want; on a shared server one unexpectedly large job should not be able to
take everything else down with it.

```bash
duckle-runner --pipeline ./daily.json   --memory-limit 24GB --threads 8   --temp-dir /data/duckle-tmp --max-temp-size 300GB
```

`--max-temp-size` is the one worth setting deliberately. **DuckDB's own
default is 90% of available disk space**, so without it a single large join or
sort can fill the volume the OS is on - which is an outage, not a slow
pipeline. `--memory-limit` is a spill threshold rather than a hard ceiling:
above it DuckDB writes to the temp directory and keeps going, so the limit
buys predictability, not failure.

Each run spills into its own subdirectory of `--temp-dir`. Pointing several
concurrent runs at one shared directory is what a person does to move spill
onto a bigger disk, and it used to make them unsafe: four concurrent spilling
queries sharing a directory lost 3 of 12 to segfaults and delete failures.

The flags set the same variables the engine reads (`DUCKLE_MEMORY_LIMIT`,
`DUCKLE_THREADS`, `DUCKLE_TEMP_DIR`, `DUCKLE_MAX_TEMP_DIR_SIZE`), so a flag, a
workspace-wide export and a per-stage setting all land in one place, with the
most specific winning.

### Poll a remote source without downloading it (`src.changed`)

A pipeline that watches a bulk source should not pay for the object to find
out whether it was needed. `src.changed` compares what a HEAD or an SFTP stat
reports against the last fingerprint it **successfully processed**, and emits
a row only for what moved. `https://`, `s3://` (including MinIO, Backblaze B2,
Cloudflare R2 and other S3-compatible stores, through a saved connection or
credentials on the node) and `sftp://`.

Two shapes, because they are the same question asked of a different number of
objects:

- **object** - one URI replaced periodically. A row when its fingerprint
  differs, nothing when it does not.
- **listing** - an `sftp://` directory or an `s3://` prefix of immutable
  files. Lists it, compares each entry, and emits the new and changed ones as
  ordinary rows for a `ctl.foreach` or an artifact copy downstream. S3 listings
  follow continuation tokens, so a prefix larger than one page is enumerated
  fully rather than silently truncated at the first thousand.

Rows carry `uri`, `name`, `size`, `modified_at`, `etag`, `fingerprint` and
`status` (`new` / `changed`).

**A quiet poll is not a plain success.** When nothing changed the node reports
`unchanged`, so a working poll and a broken one are told apart - a healthy
source can be unchanged hundreds of times between updates, and that has to
stay countable.

**Fingerprints are conservative on purpose.** None of the signals are
guarantees: an ETag can be absent, can weaken under compression, and on S3 is
a digest-of-digests for a multipart upload rather than the object's hash;
Last-Modified has one-second resolution; SFTP offers mtime and size. A missing
or unreadable signal therefore counts as **changed**. Re-reading something
unnecessarily costs compute; skipping something that did change loses data and
reports nothing.

What was processed advances only when the whole run succeeds, and only for
rows that were actually emitted - so a failure downstream re-offers the same
files, and a run capped by `maxEntries` does not mark the remainder as done.

### Maintain a DuckLake through the same pipelines (`src.ducklake.maintain`)

A lakehouse that is written to continuously eventually needs maintaining as
well as filling: frequent incremental writes leave many small files, snapshots
accumulate, and files stay referenced longer than they need to be. Those
operations used to live outside Duckle.

Each operation is **one DuckLake function**, and its options are that
function's options - compact, rewrite files heavy with deletes, expire
snapshots, clean up files an expired snapshot released, delete orphaned files,
flush inlined data, or read per-table storage statistics. Nothing here invents
storage semantics, so what it does follows the installed DuckLake rather than
anything Duckle decided.

The result comes back as **ordinary rows**, which is what lets a quality check
or an alert read a compaction the way it reads anything else, and the node
reports what changed: `ducklake compact: 1 row(s) - files 4 -> 1, 1.1 KB ->
513 B`.

Three things about deleting, since that is where this gets dangerous:

- The three destructive operations support **dry run**, which lists exactly
  what would go and changes nothing.
- Ticking dry run on an operation DuckLake cannot dry-run is **refused, not
  ignored** - an ignored dry run deletes while the operator believes nothing
  will happen.
- **Snapshot expiry does nothing without an explicit retention boundary.**
  That is DuckLake's own default and it is surfaced rather than replaced, so a
  scheduled job that forgot its boundary does nothing instead of deleting
  history.

Two maintenance runs against one catalog serialise on a lock rather than
racing, so a weekly compaction overlapping a monthly cleanup waits instead of
failing a two-hour job at its commit.

### Never buy the same row twice (item checkpointing)

The failure this exists for:

```
399,999 successful paid calls
request 400,000 fails permanently
rerun repeats all 399,999 calls
```

Tick **Remember completed rows** on `xf.ai.llm`, `xf.ai.classify` or
`xf.ai.embed` and each row's result is stored **as it arrives** - not when the
stage finishes - so a failure on the next row keeps everything already bought. A
rerun reuses them and calls the API only for what is missing.

`xf.ai.embed` needs one extra step, because its billable unit is the **batch**
rather than the row: the rows it already has are taken out first, only what is
left is chunked and sent, and everything goes back in the input order. An
embedding attached to the wrong row would be worse than paying for it twice.

The **output** is stored, not just the fact of success. A success marker without
the output leaves the item unable to run again and unable to be rebuilt, which
is not resumable at all.

Identity is the logical key **and** the whole input row **and** the stage's own
configuration - the model and prompt for `llm`, the model and **category list**
for `classify`, the model for `embed`. Asking a different question of the same
text is different work. All three, because each alone is wrong:

- a business key alone reuses the old answer for a row whose text changed
- an input fingerprint alone misses that the prompt changed underneath it

With no key named, the whole row is the key: a volatile column like a run id
then costs reuse rather than causing a wrong answer, which is the safe
direction.

```bash
duckle-runner checkpoint status                      # what each stage holds
duckle-runner checkpoint prune --retain-days 30      # bound it
```

Pruning is explicit. These entries are results that were already paid for, so
nothing is dropped on a default nobody chose.

### The feed already published its schema (XSD)

A national register hands you a 400-element XSD next to the data. Retyping it
into the Schema tab is repetitive, and a typo in it is a silently mistyped
column rather than an error.

Point `src.xml` at the XSD instead:

```
XSD file : schemas/cbe.xsd
Row path : Root/Enterprises/Enterprise
```

```
@id       bigint      <- xs:long attribute, use="required" so NOT NULL
Number    varchar     <- a named simple type, followed down to xs:string
Employees integer     <- xs:int
Turnover  decimal     <- xs:decimal stays exact; these feeds carry money
StartDate date        <- xs:date
Active    boolean     <- xs:boolean
```

Only read when the Schema tab is empty, so anything you declare by hand wins.

**It is read for types, not as a gate.** Nothing is validated against the XSD at
run time: full-document validation on every production load is expensive and is
not what the schema is wanted for. It is wanted so the bounded Parquet path can
skip per-batch inference and a daily run keeps the same column types.

The derived schema describes what Duckle's reader **produces**, which is not
quite what the XSD describes: attributes arrive as `@name`, a repeated child as
an array and a nested child as an object, so those are declared as text.
Deriving the abstract XSD shape instead would produce casts that fail on every
row.

The **exact schema bytes** are recorded in the signed run manifest, alongside
every other input the run read:

```json
{"role":"input","name":"xsd","uri":"schemas/company.xsd",
 "sha256":"eb1803ae...","sizeBytes":812}
```

A configured path can stay the same while the bytes behind it change, and the
derived column types change with them. The path alone would say nothing about
which schema a given run actually used.

**An `xs:import`, `xs:include` or `xs:redefine` is followed**, under rules that
keep a schema set from becoming a way to read the disk or the network:

- Resolved relative to the document that **named** it, not to the root, so a
  nested set loads the way its author laid it out.
- Confined to the root schema's own directory. `..` is folded *before* the check,
  because checking a path and then normalizing it is how a confinement is walked
  past.
- A **local** schema set may not fetch over the network. A remote one resolves
  through the shared HTTP agent, which is where the workspace network policy
  applies, so an import is subject to the same allowlist as anything else.
- A cycle loads once and stops, and a document shared by two parents is read
  once.
- Ceilings: 64 documents, 16 levels deep, 8 MiB in total.
- Every imported document is hashed into the run manifest alongside the root. A
  change to any of them changes the derived columns just as much.

**The whole set is one parser contract.** A publisher can replace the bytes
behind a URL that never changed, and the next run then parses the feed into
different columns and publishes the result as though nothing happened. The
manifest records what was used, but only after the data is out. So the resolved
set is fingerprinted, and the fingerprint is remembered the first time it is
seen, in `.duckle/xsd_contracts`:

| `xsdChangePolicy` | on a change |
|---|---|
| `warn` (default) | says so once, accepts the new set, run continues |
| `fail` | refuses the run until you accept it by deleting the line |
| `allow` | does not look |

The fingerprint covers **every** document in the closure, not the root, because
an `xs:include` three levels down decides a column's type just as much - a root
whose bytes never moved is no evidence that anything held. It is canonical, so
a schema that merely reorders its own imports is not a change.

Two things are still refused, because the alternative is a column list that
quietly stops early. An import naming a namespace with **no `schemaLocation`**
has nothing to resolve. And two schemas declaring a *different* type under the
same local name cannot both be honoured, so the run says which name clashed
rather than picking one and changing a column's type silently.

### A fan-out over two million parents

`src.rest` with a **URL per upstream row** turns `/companies` into
`/companies/{id}/officers` - one request per parent, one relation out. At
registry scale three things about that mattered, and none of them were the
requests themselves.

**Memory.** The fan-out used to hold every child row in one list until the stage
ended, so a 2M-parent walk was bounded by RAM rather than by the API. It now
writes as each walk finishes. Memory is the parent list, the walks in flight and
one write batch; the total number of children is no longer in that sum.

**Concurrency.** **Requests in flight** puts N parents in the air at once.
Workers pull the next parent rather than being handed a slice, so one slow
endpoint does not leave the others idle at the end of their share.

```
c  ok (8 rows) - rest: materialized 8 rows (8 page(s)) into c (unordered: 4 requests in flight)
```

Above 1, **the output order is not the upstream order** - rows land as their
requests finish. That is said in the field, in the node's message and here,
rather than left to be discovered when a downstream `LIMIT 10` returns different
rows on a rerun. **Carry upstream column** is what makes a child row traceable
without order, which is why it exists.

**One bad parent.** **When a row request fails** chooses:

| | |
|---|---|
| `Stop the run` | the default, and what it did before |
| `Skip that row` | drop it and carry on |
| `Send it to the reject output` | carry on, and keep the failure as a row |

```
parent_key | url                                  | error         | failed_at
2          | http://.../companies/2/officers      | REST HTTP 500 | 2026-08-28T...
```

One failure in a million requests should not discard the 999,999 that worked,
and a run that half-failed is only operable if the failures are somewhere you
can query rather than only in a log. The reject relation is built even when
empty, so a node wired to it binds on a clean run too.

`_page_number` is now per parent walk. A global counter said 4001 for the first
page of the 4001st company.

### A fan-out that died at row 900,001

Tick **Remember completed rows** on `src.rest` and each upstream row is recorded
as its requests finish. A rerun does not fetch it again:

```
c  ok (3 rows) - rest: materialized 3 rows (0 page(s)) into c,
                 3 parent(s) reused from the checkpoint
```

It is the **same store the AI steps use**, on purpose. A fan-out with its own
record of what succeeded would be a second answer to the same question, and two
records of that kind drift apart. Resume falls out of the execution shape rather
than sitting beside it.

Identity is the carried parent key when there is one and the whole upstream row
otherwise, plus everything that shapes the request - URL, template, method, body,
response path, and the saved incremental cursor. Change any of them and the old
answers are not reused, because they answered a different question.

The destination may be **`s3://`**, using the object-storage connection on the
node, so a raw zone can be the raw zone rather than a local staging step. It is
written before the parse either way, so no parsed row can exist without its
source being durable - which a later copy stage could not promise.

`src.html` takes the same setting. It already fetched once, so the archived page
and the parsed page are the same bytes rather than two requests that might
differ, and its rows carry the same two columns.

Each parsed row carries `_response_uri` naming the artifact it was parsed out
of, so nothing downstream re-derives the destination template - which `{date}`
would not reproduce on a run that crossed midnight anyway. The artifact is
written **before** the parse, so a row can never name a file that does not exist.

### Following pagination a server rendered

Set a **Next-page link** selector on `src.html` and Duckle follows the link the
page names, then the one that page names, until a page names none:

```
Next-page link : a.next
Link attribute : href
Max pages      : 100
```

Relative links resolve the way a browser resolves them - against the page URL,
or a `<base href>` when the document sets one. A bare `?p=2` keeps the path and
replaces the query; `../next` climbs one directory. Getting that wrong does not
error, it silently fetches the wrong page, so each shape has its own test.

A walk that stops EARLY - a page that failed, or the page cap reached with a
link still to follow - reports the run as **incomplete** and stops anything
downstream, the same as a budget stop. Skipping a document in a corpus loses
that document; skipping a page in a chain loses every page after it, because
the link to them was on the page that failed.

Bounded three ways, because the link is written by someone else: a page cap, a
stop when a page names no link, and a stop when a URL **repeats**. That last one
matters most - a next link pointing back at page 1 is a cycle, not a long list.

Every page goes through the same transport, capture and provenance as the first,
so `_response_uri` and `_response_sha256` identify the page a row came from.

Ignored when documents are wired in from upstream: that list already names every
page it wants, and following links out of it would fetch pages nobody asked for.

### Handing a scanned page to your own OCR

Duckle does not do OCR, and will not: rasterising needs a native rendering
engine plus per-language trained data, which would end the self-contained
cross-OS build. What it owes instead is a page you can render **without
guessing** - so a page with no text layer arrives carrying everything an
external stage needs.

```
src.changed  ->  xf.artifact.copy  ->  src.pdf  ->  filter  ->  code.python
                 (localise)            (pages)     (no text)   (your OCR)
```

```sql
SELECT document_uri, page_number, source_sha256
FROM pages
WHERE has_text_layer = false
```

| column | what the OCR stage does with it |
|---|---|
| `document_uri` | opens it |
| `page_number` | renders that page |
| `source_sha256` | pins the bytes, so a re-render is reproducible |

Then, in **your** locked environment - the one `uv.lock` pins, so the render is
the same next month:

```python
import fitz  # PyMuPDF

def process(row):
    page = fitz.open(row["document_uri"])[int(row["page_number"]) - 1]
    row["image_path"] = f"/work/{row['source_sha256']}-{row['page_number']}.png"
    page.get_pixmap(dpi=300).save(row["image_path"])
    return row
```

**Localise before OCR.** That is the one constraint. When `src.pdf` fetches a
remote document it spools, parses and deletes - one document at a time, so the
bound is a document rather than the corpus - and `document_uri` is then the
remote URI, which a Python step cannot open. Re-fetching it would be neither
stable nor reproducible: a URL is a name that can be rebound, so the second
fetch may not be the bytes that were parsed. `xf.artifact.copy` first, and
`document_uri` is a local path.

PyMuPDF, Docling, PaddleOCR and Tesseract all work from that pair, and none of
them become Duckle's problem.

### A cursor that reaches the request

Filtering after the fetch is not incremental for an API. You still pay for the
whole dataset every run, so for a large API it is a full reload with extra
steps. The cursor has to reach the request.

Name an **Incremental field** on `src.rest` and put `{incremental}` wherever the
API takes its cursor - the URL, a query parameter, the body or a header:

```
URL                  : https://api.example.com/changes?since={incremental}
Incremental field    : updated_at
Starting value       : 1970-01-01
```

```
run 1  GET /changes?since=1970-01-01   -> records up to 2026-03-05
run 2  GET /changes?since=2026-03-05   -> only what is new
```

The mark is the **highest value seen**, not the last one received - an API that
returns a page out of order must not move the cursor backwards and re-fetch
what was already taken. Numbers compare numerically and everything else
lexically, which is why ISO-8601 works without a date parser.

**It is saved only when the whole pipeline succeeds**, through the same deferred
queue every other watermark uses. A run that fails after this stage does not
advance the cursor past rows no sink ever received. Nothing REST-specific was
added for that; it is the mechanism `xf.incremental` and the Kafka resume point
already use.

`{incremental}` is a reserved name. When the node is also fanning out over
upstream rows, an upstream column called `incremental` is refused rather than
silently shadowed by the mark.

### A ceiling on the bill, not just on the rate

Rate limiting controls how fast money leaves. It does not control how much. A
prompt template that accidentally embeds a whole document, over a source that
grew tenfold overnight, is a bill nobody approved.

`xf.ai.llm`, `xf.ai.classify` and `xf.ai.embed` take a **Budget**:

```
Max requests                      : 1000000
Max input tokens                  : 500000000
Max output tokens                 : 30000000
Max estimated cost (USD)          : 50
Input price per million tokens    : 0.15
Output price per million tokens   : 0.60
```

Reaching one stops the stage. What happens then is the part worth reading:

```
status     : ok
incomplete : budget:maxRequests - the rows produced are correct and are not all
             of them; nothing downstream ran
  l   ok (5 rows) - ai.llm: stopped at the budget:maxRequests ceiling after
                    2 request(s), 20 input + 10 output token(s)
  k   skipped     - not run: an earlier stage stopped at its budget
```

- **Not a failure.** The rows already bought are correct and paid for. `status`
  stays `ok`, and a Plan step does not fail.
- **Not a plain success either.** `incomplete` sits beside `status` with a
  machine-readable reason, because alerting has to tell "we hit the ceiling"
  apart from "it broke", and a sentence cannot be matched on.
- **Everything downstream is skipped.** This is the point. Stopping is only the
  mechanism; the damage a budget stop prevents is a sink publishing two rows of
  five as if they were the answer.
- **No watermark advances.** An incomplete run read a window and processed part
  of it. Recording the end of that window would make the next run skip
  everything the budget stopped, permanently. This is the one place where "not
  a failure" still has to behave like one.
- **The checkpoint keeps what was bought.** Tick **Remember completed rows** and
  a rerun after raising the ceiling calls the API only for what is missing.

**How exact is it?** The request ceiling is exact: no request past the Nth is
ever issued, and it holds under concurrency (the slot is claimed with a
compare-and-swap, so eight workers cannot all take the same last one). Token and
cost ceilings cannot be, because tokens are only knowable after a reply. The
guarantee is precisely: **no request starts once the recorded totals have
reached the limit**, so the last one may carry the total past it by at most one
request's worth. Anything stronger would need a local tokenizer per model and
would still be an estimate.

A cost ceiling **with no prices does not compile**. It could never be reached,
and a limit that cannot fire is worse than no limit: it is a limit somebody
believes in. `duckle validate` catches it before a run rather than a run
discovering it after the first stage has started spending. Request and token
ceilings need no prices and work against a self-hosted endpoint.

### Extraction should produce columns, not a paragraph containing the answer

`xf.ai.llm` can ask for a shape instead of prose. Pick **A JSON Schema you
define**, paste the schema, and the provider enforces it while it writes
(`strict: true`). Tick **Turn the reply fields into columns** and each top-level
field lands as its own column, ready for a join, rather than a JSON blob a
downstream stage has to unpack.

The reply is checked again locally, and that is the point. An
OpenAI-**compatible** endpoint may accept `response_format` and ignore it, and a
silently unstructured answer is exactly the failure this removes. What is
checked: the reply parses, every `required` field is present, and each declared
top-level field is the type the schema says. Nested schemas are enforced by the
provider during decoding; re-implementing draft 2020-12 here to check them a
second time would be a large dependency for a second opinion, and it is not
claimed.

Three things are refused before a single request is billed:

- a schema that does not parse
- a reply shape of JSON Schema with no schema given
- a schema field with the same name as an incoming column, which expansion would
  silently overwrite

**When a reply does not match** defaults to stopping the run. An extraction that
quietly produced nulls for a tenth of its rows is worse than one that stopped;
the other setting is there for genuinely messy input.

The schema is part of the checkpoint identity, so adding a field to it does not
hand back yesterday's answers, which do not have it.

### Two machines that both look correct (Python environments)

```
machine A    .venv with splink==4.0.0
machine B    .venv with splink==3.9.1
```

Same pipeline, same `uv.lock`, different answers, and nothing anywhere says so.
Duckle now reads the environment as it **is** - the distributions installed in
it - rather than trusting a marker written by whoever created it. A stamp file
records an intention; `*.dist-info` records the fact.

Commit a `uv.lock` and the check turns on. A pipeline with a `code.python` node
is refused before it runs when the workspace `.venv` contradicts the lock:

```
error : python: the workspace .venv is not the environment uv.lock describes,
        so this run would not be the run the lock says it is:
          splink: installed 3.9.1, locked 4.0.0
```

```bash
duckle-runner python check      # exit 1 when it does not match, so CI can gate
duckle-runner python prepare    # rebuild .venv from the lock (uv sync --frozen)
```

**Nothing is installed during a pipeline run.** Resolving dependencies mid-run
would turn a missing package into a download, which an air-gapped box and a
scheduled job cannot have. `prepare` is a provisioning step: run it once, in CI
or at deploy time.

A package the lock names but that is not installed is **reported and does not
fail**. A lock resolves for every platform, so something absent here may simply
not apply here; a package that really is needed and really is missing raises
`ImportError` on the first row, which is already unambiguous. What does fail is
a version that contradicts the lock, or a package the lock never mentions -
those are the two shapes of "someone changed this environment".

**A deployed pipeline cannot silently run against an unprepared target.** A
bundle built from a workspace with a `code.python` step carries `uv.lock` and
`pyproject.toml`, so the target has something to verify against. On that target,
a lock with nothing installed in `.venv` is refused before the run starts:

```
error : config: python: /srv/app declares a locked environment (uv.lock) but
        nothing is installed in .venv, so this target is not prepared. Run
        `duckle-runner python prepare` on this machine before the pipeline runs,
        or point DUCKLE_PYTHON_BIN at the interpreter you mean.
```

That gap mattered: "missing packages" alone is deliberately not a failure, so an
absent or never-synced `.venv` produced only Missing entries and the run went
ahead against whatever Python the machine had. The lock is shipped, **not** the
environment - preparing the target stays an explicit step, because resolving
dependencies at run time is what an air-gapped box cannot have. Naming an
interpreter with `DUCKLE_PYTHON_BIN` is a decision, so it is exempt.

`DUCKLE_PYTHON_ALLOW_DRIFT=1` downgrades the refusal to a warning. There is
always a machine where the rule is wrong, and a check with no way past it gets
deleted rather than fixed.

With no `uv.lock`, none of this applies and `code.python` behaves exactly as it
did: `DUCKLE_PYTHON_BIN`, then the workspace `.venv`, then the system Python.

The signed run manifest records the Python version, the platform, the `uv.lock`
SHA-256 and the environment hash - but only for a pipeline that actually has a
Python stage, so an unused interpreter does not become noise in every manifest.

### Do not parse the same 40,000 PDFs twice (output caching)

The checkpoint above remembers each **item** as it is bought. This remembers the
**whole relation a stage produced**, so a stage whose inputs and settings have
not changed does not run at all - the parse, the script, the extraction, none of
it.

Tick **Skip this step when its inputs have not changed** on `src.pdf`,
`src.xml`, `src.html`, `code.python`, `code.javascript` or `code.wasm`. On the
next run the stage's output is served from the workspace cache instead of being
recomputed, and the node says so:

```
j  code.javascript  ok  reused cached output 3f9a1c22b4d0
```

It is off unless you ask for it, and it is deliberately hard to fool:

- The key is the component, the node's settings **and** a checksum of the rows
  arriving from upstream. Change any of them and the stage really runs.
- Secrets are stripped out of the key. A rotated password is not new work.
- **No upstream connection means no caching.** A stage reading the outside world
  has no input this pipeline can checksum, so keying on settings alone would
  hand back last week's parse of a file that has since changed. It is refused
  instead of guessed at.
- **A Python stage is keyed on its environment too**, and refused without one.
  The same script under a different pyarrow is different work; a stage running
  against whatever interpreter the machine happens to have has no identity to
  pin, so it is not cached at all.
- **An engine upgrade invalidates everything.** A stage is deterministic given
  a build: a parser fix makes the same input produce a different, better answer,
  and a cache that survived the upgrade would quietly keep serving the one the
  fix was meant to correct. That costs one slow run, which is the right side to
  err on.
- The list of components is an allowlist, not a denylist. Anything that writes
  somewhere, reads a clock or talks to a queue gives a different answer the
  second time, and reusing the first one would be wrong rather than fast.
- A cache that cannot be read or written is a slower run, never a failed one.

```bash
duckle-runner cache list                    # what is cached, by pipeline and node
duckle-runner cache clear --pipeline daily  # drop it
duckle-runner --pipeline p.json --no-cache  # distrust it for one run
```

`--no-cache` neither reads nor writes, so a run taken to settle whether the
cache is lying does not then overwrite the evidence.

Cached output lives under `<workspace>/cache/<pipeline>/<node>/` and can be
deleted at any time. Unlike the checkpoint above, nothing here was paid for -
it can all be recomputed, which is why clearing it needs no ceremony.

### A JSON column that appears late is not a column you lose

DuckDB decides what a JSON document's columns ARE from the first `sample_size`
records - 20480 by default. On records that do not all carry the same keys, that
**silently drops** every column first appearing later: the read succeeds, the
rows look right, and a field is simply gone. No error, nothing to notice.

`src.json` now scans everything by default (`sampleSize: -1`). That costs an
extra pass over the file, which is the honest price of not losing columns. Set a
number if you know your records are uniform and would rather have the speed.

### Make forbidden things impossible, not discouraged (workspace policy)

Roles answer *which control-plane actions may this key invoke*. A different
question matters once an AI agent or a CI job can write pipelines: **which
capabilities may a pipeline in this environment contain at all**. An agent with
legitimate write access to the repository defeats the first entirely.

"Do not modify production data" in a prompt is guidance. A policy file is a
boundary.

```yaml
mode: enforce
components:
  deny: [code.shell]
network:
  allowedDomains: [api.registry.example]
sinks:
  allowedConnections: [dev_lake]
  allowedS3Prefixes: ["s3://company-development/"]
  deniedSchemas: [production]
filesystem:
  allowedPaths: [/var/lake/dev]
state:
  allowMutation: false
```

`DUCKLE_POLICY_FILE` points at the authoritative one, from outside the
workspace. `.duckle/policy.yaml` may then add restrictions.

Four things make it a boundary rather than a check:

**Enforcement is at plan time, and again at the point of the act.** An agent
that can write the pipeline can also invoke a path that skips a validation step,
so the check sits where a pipeline becomes executable - a denied capability has
nowhere to run, rather than having failed a check somebody can route around.
Nothing is written before the refusal.

Plan time alone is not enough, because a plan-time check reads a URL out of a
node's properties and the request happens somewhere else entirely. So each rule
also holds where it is actually exercised:

| rule | also enforced at |
|---|---|
| `network.allowedDomains` | every connection, every redirect hop, and DuckDB itself |
| `state.allowMutation` | the watermark writers, so `duckle-runner backfill`, the API, MCP and the panel all meet the same refusal |
| `extensions.allowUnsigned` | the DuckDB launch, which can withhold `-unsigned` but never grant it |

DuckDB is the reason that last one names two enforcers. Duckle's own HTTP
client checks every host it dials, but SQL that DuckDB runs never passes
through that client: `read_parquet` over https, a remote `ATTACH`, a `COPY` to
a remote URI, an extension that fetches on its own. Scanning the SQL for those
is not a boundary, because the SQL is generated by dbt, Python, templates and
MCP, and a path can be built at run time from a row. So under a restricted
network the run starts with DuckDB's remote filesystems disabled and community
extensions refused, both of which DuckDB will not let a later statement undo.
Local file access is untouched. Where an operator genuinely needs DuckDB-native
remote reads, `network.allowDuckdbExternalIo: true` in the server policy returns
them, along with the boundary they cost.

**Prefixes match at a boundary, not as strings.** An allowed path of
`/var/lake/dev` does not admit `/var/lake/development`, and an allowed
`s3://co-development` does not admit `s3://co-development-prod`.

**Narrowing is the only operation the format has.** Denies union, allowlists
intersect, permissions AND. There is no expressible way to remove a deny or
extend an allowlist, so "a workspace may never widen a server policy" is
structural rather than a merge rule somebody has to keep getting right.

**`mode` comes from the server policy alone.** The workspace file is writable by
whatever writes the pipelines, so a workspace that could set `mode: report`
could switch the boundary off from inside the thing being bounded.

A policy file that is named and cannot be read **refuses the run**. Falling back
to "no policy" would mean a typo in the environment silently removes the
boundary. With no policy configured at all, nothing changes.

### Catch the run that looks fine and is not (`qa.baseline`)

Absolute rules catch a NULL where one is not allowed. They cannot catch this:

```
Monday       5,120,310 rows
Tuesday      5,131,244 rows
Wednesday    5,129,991 rows
Thursday       842,114 rows
```

Every one of those 842,114 rows can satisfy the schema and every row-level
rule. The pipeline stays green and publishes, which is more dangerous than a
crash - a crash is noticed.

`qa.baseline` profiles the current input, compares it against the **median** of
the last N accepted profiles, and either reports the comparison as rows or
fails the run. Median rather than mean, so one odd day does not drag the
baseline towards itself.

Row count, and per column the null count, null rate, distinct count, min, max
and mean. Rules take limits in either direction, as a percentage or as an
absolute: a null rate going from 0% to 5% is an infinite percentage increase,
so a percentage says nothing about it.

`groupBy` with `requireExistingGroups` catches a partition that stopped
arriving - a country missing from a feed - **even when the total row count is
unchanged**, because the other partitions grew to cover it. A dataset-level
rule cannot see that at all.

Deterministic throughout: rolling summary statistics and explicit thresholds,
no model. Only compact numbers are stored, never copies of the data. And the
new profile is accepted **only if the whole run succeeds**, so a bad day never
teaches the gate that bad is normal.

**Re-basing it when the source really did change.** A retired product line, a
migrated system, a rate that genuinely moved - the accepted history now
describes a world that is gone, and every run fails against it. A gate with no
way to say "this is the new normal" gets deleted, or has its thresholds widened
until it means nothing, which is worse because it still looks like a check.

```bash
duckle-runner baseline list                                    # what has a baseline
duckle-runner baseline inspect --pipeline orders --node qa     # accepted vs last run
duckle-runner baseline accept  --pipeline orders --node qa     # this is the new normal
duckle-runner baseline clear   --pipeline orders --node qa     # start the history over
```

`accept` promotes what the **last run measured**; it never invents a number, so
a node no run has measured has nothing to accept. That works because a refused
run still records its profile - the run an operator most needs to look at is
exactly the one the gate rejected, so that observation is written whatever the
outcome, unlike the accepted history.

Both `accept` and `clear` go through `state.allowMutation` and are written to
the audit log with the value they replaced, because "somebody cleared it" is not
reviewable and the number it held is. The same four operations exist over the
HTTP API and MCP.

### A corpus, not one file (`src.xml` on the artifact contract)

Wire an artifact relation into `src.xml` and it reads every document that
relation names, instead of one configured path - so change detection, an
immutable landing copy and the parser compose instead of each needing its own
notion of "where the file is":

```
src.changed  ->  xf.artifact.copy  ->  src.xml  ->  DuckLake
```

Each document is **streamed straight out of the artifact reader**. The pull
parser never seeks, so spooling every file to disk first would buy nothing and
cost a full local copy of the corpus. Object storage works on this route (it
goes through the signed S3 read), which the configured-path route still cannot
do. A zip is refused with a pointer to `xf.archive.extract`, because a zip
directory is at the END of the file and cannot be streamed.

`carryColumns` copies the business keys - `company_id`, `filing_id` - onto every
row the parser emits, so a row can be joined back to the document it came from
without a second lookup, and `source_sha256` is carried rather than recomputed.

**The corpus list is bounded too**, not just each parse. Reading the artifact
relation into memory before opening the first document would make a million-row
corpus cost memory proportional to the corpus - a fix that looks complete and is
not. The list is materialised once into the run database and read back a batch
at a time, numbered rather than paged with a bare `LIMIT`/`OFFSET`: a view with
no `ORDER BY` can hand back a different order on the next call, and a corpus
that silently repeated or skipped documents is worse than one that would not fit
in memory. `src.pdf`, `src.xml` and `src.html` all go through it.

`src.html` takes the same contract, for the case where the corpus is pages
rather than documents. It reads each page whole rather than streaming, because
a CSS selector needs the DOM built before it can match anything - there is
nothing streaming would save.

One writer serves the whole corpus, so the bounded-parts machinery below bounds
**all** the documents rather than each one: a million small files cannot do what
one huge file already could not. `onError: skip` keeps going past a document
that will not read, and says how many it skipped - a corpus that quietly lost
documents is the failure this contract exists to prevent.

### Bounded materialization for large XML (`src.xml`)

The XML parser is a pull parser, so live memory is one row plus the nesting
depth however big the file is. The intermediate was not bounded the same way:
every parsed row went to one NDJSON file that grew to the size of the whole
result, and NDJSON repeats every property name on every row - so a 30GB
compressed source could put hundreds of gigabytes on the temp volume.

**With a declared schema**, rows are rolled to a compressed Parquet part every
250,000 rows (`batchRows`), the NDJSON only ever holds the tail, and the parts
are read back as one relation. The uncompressed intermediate is bounded by one
part rather than by the result, and the rest is columnar, so the property names
are stored once per part instead of once per row.

The declared schema is what makes this available: each part is typed as it is
written, because two parts inferring different types for the same column would
fail to union at the end. The node reports how many parts it took, so the
bounding is visible rather than assumed.

### Unpack an archive into artifacts (`xf.archive.extract`)

Bulk data is published as archives far more often than as readable files, and
unpacking one used to mean a shell stage. As an **artifact operation** rather
than something built into each parser, a ZIP of CSVs, a TAR of JSON and a GZIP
of NDJSON all land the same way and each member then flows into whichever
parser suits it.

One archive row in, one artifact row per member out: `archive_uri`,
`member_name`, `member_index`, `uri`, `media_type`, `compressed_size`,
`size_bytes` and `sha256`. ZIP, TAR, TAR.GZ and GZIP.

TAR and GZIP are read front to back and stream straight from the source, so an
archive nobody has to hold is an archive whose size does not matter. A ZIP is
spooled one at a time, because its central directory is at the END of the file
and a reader has to seek.

Two things about untrusted input, because an archive from an external publisher
is exactly that. **A member path can never escape the destination**, however the
archive names it. And an archive is a compression format, so a small one can
expand to fill a volume - the expansion limit is applied **while reading**,
which refuses rather than discovering it from a disk-full error.

### Land the bytes somewhere durable (`xf.artifact.copy`)

An artifact is a reference - a uri, a media type, a size, a hash - so a
pipeline can carry one around for nothing. At some point the actual bytes have
to move, and that is this step: between "the feed says there is a new 4GB
bundle" and "it is in our raw zone, hashed, and we can prove which bytes we
parsed".

It reads a `uri` column - whatever `src.changed`, `src.artifact` or a query
produced - and copies from `https://`, `s3://`, `sftp://` or a local path to
an `s3://` prefix or a local directory.

**Streamed and hashed in one pass.** Memory is bounded by the part size rather
than by the object, so a 40GB model file does not become 40GB of RSS, and the
`sha256` recorded is of the bytes that actually transferred. Reading twice -
once to hash, once to upload - would double the transfer off a remote source;
hashing first would mean holding the whole thing.

Naming is `keep` (the source's file name), `path` (its layout preserved under
the prefix) or `hash` (content-addressed, which makes the store immutable and
de-duplicating at the cost of reading each source twice, because the key is
the hash). A source-derived name can never climb out of the destination
prefix.

`ifExists: skip` is the default and is what a raw zone wants: re-running a
feed does not re-upload what already landed. The row still comes out, with
`copied = false`, because downstream still needs to know the artifact exists.

Emits `uri`, `source_uri`, `name`, `media_type`, `size_bytes`, `sha256` and
`copied`.

**Remote artifacts reach the signed run manifest.** `.ducklock` pinned local
file inputs by path, and a remote object has no path - so the boundary that
matters most in a raw-zone pipeline, where the bytes came from, was the one
thing the manifest did not record. Every object a run reads or writes now
appears in it with its uri, size, media type, and either a `sha256` when the
bytes actually passed through the run or an ETag and mtime when they did not.
An object that was merely observed carries no hash, because claiming one would
be a lie. The manifest also records the resource limits the run was given, so
two runs that spilled differently can be told apart from two runs handed
different budgets.

### Tumbling windows that survive between runs (`xf.tumble`)

Aggregating a stream by time needs a window to stay open across batches, and
needs to know when it can be closed. `xf.tumble` assigns each row to a
fixed-size bucket by its EVENT time, holds it until the bucket closes, then
emits it with `window_start` / `window_end` for an ordinary `GROUP BY`
downstream.

Closing is decided by a **watermark** - the greatest event time seen so far,
across runs - not by the wall clock. Replaying last year's data therefore
produces last year's windows, instead of finding every one of them older than
"now" and closing the lot at once.

`allowedLateness` holds a window open past its end for out-of-order arrivals.
Anything that arrives after its window was already delivered is **dropped and
counted**, not emitted: sending it would hand a downstream consumer a second,
partial copy of a window it already has, with different numbers in it.

The rows in still-open windows and the watermark ride the same deferred flush
as every source position, so a batch that fails downstream leaves them intact
and re-processes rather than losing what it was holding.

### Web panel (remote management console)

To run and monitor pipelines on a server with a browser instead of the desktop app, start the built-in **web panel** - it is part of the same `duckle-runner` binary, so there is nothing extra to install:

```bash
duckle-runner serve --port 8080 --workspace /path/to/workspace
```

Open `http://localhost:8080`. The panel has eight views:

- **Overview** - every pipeline with its last status, duration and next scheduled run, and a Run button.
- **Runs** - run history across every pipeline (status, duration, rows, errors) with expandable per-pipeline run logs and optional auto-refresh.
- **Schedules** - an editable cron or interval schedule per pipeline, showing what is running now and what is due next.
- **Plans** - several pipelines in the order you chose. See [Plans](#plans-several-pipelines-in-an-order-you-chose).
- **Catalog** - everything the workspace reads and writes, who owns it, and what is written but never read. See [Workspace catalog](#workspace-catalog-what-reads-and-writes-what).
- **Batches** - work queued for workers: progress, what is running now, what failed, and a retry for the failures.
- **People** - the accounts that may sign in and the keys machines use, with the role each one has. Admin only.
- **Audit** - who signed in, what they changed and who was turned away. Admin only, and shown only to admins.

Runs execute in-process through the same engine, are written to the same run history (`<workspace>/runs/`) and logs (`<workspace>/logs/`), and a built-in scheduler triggers any pipeline whose schedule has elapsed - so the server itself runs your schedules, no OS cron needed.

#### Plans: several pipelines, in an order you chose

A schedule runs one pipeline. A **plan** runs several, in steps: everything inside a step goes at once, and the next step waits for it. A step that fails stops the ones after it, so nothing runs against data that was never produced.

That is the shape most nightly loads already have. Without it they get written as three schedules set a few minutes apart and hoped over, which works until the extract takes four minutes instead of two.

Build one wherever you are: the **Plans** tab in the web console, or the **Plans** tile under Operate in the desktop app. Add a step, put pipelines in it, and the card draws the chain it will run.

```
EXTRACT                    PUBLISH
orders.json      -->       export.json
customers.json
```

Two things worth knowing:

- **Every pipeline keeps its own run history.** A plan does not collapse into one opaque run, because at three in the morning the question is which step broke, not that the nightly load did.
- **A plan can be scheduled like anything else**, from its own card. The same plan runs whether the schedule is fired by `serve` on your server or by the desktop app on a shared workspace - both read `plans.json` and `schedules.json`, and both decide it the same way.
- **It is one file, so it travels.** A plan written in the desktop app opens in the console and the other way round, and it goes to your server with everything else in the workspace.

Plans live in `<workspace>/plans.json`, so they are a file in git alongside the pipelines they order.

#### Sign-in and roles

Start from where you actually are.

**Running it on your own machine?** Nothing to do. On `127.0.0.1` with no accounts the console is open, because anyone who can reach it is already sitting at the machine, and asking them for a password would protect against an attacker who has already won.

**Put it on a server and it refused to start?** That is the feature. The console can run any pipeline in the workspace, and a pipeline can run shell and SQL, so reaching it is the same as running code on that host. A bind it cannot authenticate fails rather than serving anyone and printing a warning nobody reads. The shortest way past it:

```bash
DUCKLE_CONSOLE_TOKEN=<secret> duckle-runner serve --host 0.0.0.0 --port 8080
```

**More than one person?** Give each of them their own, so the audit log can name them. The token is printed once and kept only as an Argon2id hash:

```bash
duckle-runner console add-user reporting --role viewer
duckle-runner console add-user ops       --role operator
duckle-runner console list
```

**A machine needs in?** CI, a scraper, or your own laptop deploying: those have no browser and nobody to rotate a password, so they get a key instead of an account. See [API keys](#api-keys-for-machines).

| Role | Can |
|---|---|
| `viewer` | Read the dashboard, run history, logs, schedules and catalog. |
| `operator` | Everything a viewer can, plus run pipelines and change schedules. |
| `admin` | Everything an operator can, plus deploy pipelines, connections, credentials, the audit log and the workspace itself. |

The split follows what an action can destroy, not which screen it lives on. It is why **deploying a pipeline needs `admin` while turning its schedule on needs `operator`**: shipping code to a host and deciding when trusted code runs are different sizes of decision.

#### How a request is decided

Three ways to prove who you are, one identity, one check, and every outcome recorded:

```mermaid
flowchart LR
    R([Request]) --> C{"Session cookie?"}
    C -->|within 12h| ID["Identity<br/>name + role"]
    C -->|no| B{"Bearer token?"}
    B -->|API key| ID
    B -->|account token| ID
    B -->|nothing| U["401<br/>sign in"]
    ID --> P{"Role enough<br/>for this route?"}
    P -->|yes| OK["It happens"]
    P -->|no| F["403<br/>refused"]
    OK --> A[("audit log<br/>who, what, when")]
    F --> A
    U --> A
```

Two things follow from that shape. **A refusal is recorded as carefully as a success**, so `audit --outcome denied` answers "who is reaching for what they do not have". And **a route with no entry in the permission table needs `admin`**, so a route added later is locked down rather than accidentally left open.

A browser trades its credential for a session cookie, so it never stores the credential itself: the cookie carries a random session id, is `HttpOnly` and `SameSite=Strict`, is marked `Secure` when a proxy tells Duckle the browser is on https, and lasts 12 hours. Sessions survive a restart, so a rolling deploy does not sign your team out.

Accounts, sessions and keys live in `<workspace>/.duckle/console.db`. Nothing in it can be replayed: an account token is an Argon2id hash, and a session id and an API key are both generated with 256 bits of entropy and stored as SHA-256, so a copy of the file or a backup of the workspace admits nobody. An older `console-users.json` is carried in on first start and renamed `.migrated`, so upgrading neither locks anyone out nor destroys the only copy of a credential store. The same accounts, roles and keys cover `duckle-runner web`.

Read it back from the **Audit** view, or from a terminal with no server running:

```bash
duckle-runner audit                                  # newest first, 50 by default
duckle-runner audit --outcome denied                 # who reached for what they do not have
duckle-runner audit --actor ops --action schedule    # one person, one family of actions
duckle-runner audit --limit 500 --json               # for a collector
```

`allowed` means the caller was permitted to proceed, not that the work then succeeded - run history answers that. Reads are not recorded, so a dashboard polling every few seconds does not bury the entries worth seeing. A page says when older entries exist beyond it, and a line that will not parse is counted rather than silently skipped.

Still put it behind a reverse proxy if you need TLS.

### Migrating a repository of legacy jobs

The editor imports one job at a time, which is how you try Duckle. This is how you leave
another tool: point it at a checkout and convert everything.

```bash
duckle-runner import ./legacy-jobs                    # convert the tree into ./imported
duckle-runner import ./legacy-jobs --out ./pipelines  # somewhere else
duckle-runner import ./legacy-jobs --json             # for a migration script
duckle-runner import ./legacy-jobs --strict           # CI gate, exits 1 if anything needs a person
```

The folder layout is mirrored rather than flattened, because two jobs in different folders
routinely share a name and flattening would silently drop one. Files that are not jobs -
routines, contexts, SQL templates - are reported separately rather than counted as
conversions, and no empty pipeline is written for them.

Reusable job bodies convert alongside jobs, and a job's children resolve to the files they
became, so the master/child/joblet graph survives the move rather than arriving as a set of
disconnected pipelines. A loop or iterate body is lifted into its own pipeline that the
parent calls, which is why the file count comes out higher than the job count.

The closing tally is the number to decide on. It says how many jobs came across clean, how
many need a person, and which components have no equivalent yet, sorted by how often they
appear. On a real 125-file corpus that list had a single entry, a site-specific custom
component: coverage is the head of the distribution, so a corpus usually converts far
better than a raw component count suggests.

What remains is credentials and Java. Credentials were never in the job files: encrypted
passwords become `${ENV:...}` placeholders and connections defined outside the job are
named so you can point them at a saved connection. Java is the part that needs a person,
and the report separates it so you can see how much there is:

- A mapper expression is translated when it has one faithful SQL reading: a literal, a
  cast, a character function, a comparison, a choice, arithmetic. Anything whose meaning
  depends on a Java type the job file does not record stays reported, because guessing
  there produces a silently wrong number rather than a failure.
- A Java body is never turned into something that compiles. It imports with no SQL and
  fails, since a pipeline that runs while omitting the rules is worse than one that stops.
  A body whose every statement is a print carries no rules and is called out separately, so
  a long list sorts into what can be deleted and what has to be ported.
- A body whose every statement sets a context value is carried over instead, as one Set
  Run Variable node per value, wired in the order the body set them. All of it or none of
  it: one statement that cannot be read leaves the whole body for a person, because
  carrying half of it over leaves something that looks finished and is not. A body that
  took its values from the row it was given ran once per row, so the last row decided what
  they held, while a node sets them once from the first row - the same thing for a single
  row, which is what these bodies are usually fed, and the report says which nodes it
  applies to rather than leaving you to notice.

A component with no equivalent is imported as a named placeholder and reported. That
includes a job body's input and output ports: a child pipeline runs for its side effects,
so it does not yet take rows from its caller or hand them back.

**A SQL step that changes the database is reported, not converted.** A SQL step returns
rows and compiles into a view, so a step carrying an `UPDATE`, `MERGE` or `CREATE` cannot
become one: it would reach the database wrapped in `CREATE VIEW` and fail there. That is
knowable at import, so it is said at import. On a 125-file corpus, 16 steps.

**How a write writes is carried across.** A warehouse sink records whether it appends rows
or amends the ones already there, and importing that as the default write mode turned an
append into a full-table replace - so on a table several nodes write to, each one erased
the one before it. The write action now comes across, with the key it matches on taken
from the columns the schema marks as keys. An action with no exact equivalent here is
reported rather than widened in silence.

**The order subjobs run in is kept.** Most subjobs are not linked to each other at all;
they run one after another in the order the file lists them at the end, and that order was
being dropped. A job that wrote a table in one subjob and read it in the next then arrived
as two things that could happen in either order. Branches of a parallel fork are the one
part that genuinely does not run in declared order, so they are left out of the chain.

**Intermediate work moves to DuckDB.** A job written against a warehouse uses it as
working storage as well as a destination: it writes a staging table, reads it back, joins
it, writes it again, and every one of those hops is billed for rows that were produced on
this machine in the first place. So a table the imported project both writes and reads is
mirrored into `<workspace>/.duckle/staging.duckdb` as it is written, and the reads are
pointed at the mirror. The warehouse write is left exactly as it was, which is what makes
this safe to do unasked: every table still lands where it landed before, so nothing
downstream of the project can tell the difference. Only the reads move.

A read moves only when the whole of it can. A query that also names a table the project
does not write still needs the warehouse to resolve it, so that read stays - and so does
the staging table it reads, since a mirror would then be serving only half of what the
project asks for. Neither does a read the job could run before its own write: within one
pipeline the write has to lead to the read, by rows or by an ordering link, because a
warehouse table nothing wrote yet holds stale rows while a local one is simply not there.
A mapper's second input is judged by where its mapper sits, since that is when a lookup is
loaded and nothing feeds the lookup itself; where such a read does move it is held until
the mirror has been filled. A mapper that reads a table and produces the write back to it
keeps reading the warehouse, because what the lookup feeds is what changes the table.
Anything else, including a query assembled at run time, is mapped as it was.

### Workspace catalog (what reads and writes what)

Every other lineage view in Duckle answers about **one** pipeline. The catalog answers about the whole workspace, by joining pipelines through the assets they name: two pipelines that read and write the same table are connected whether or not anyone drew a line between them.

```bash
duckle-runner catalog lint                                        # CI gate, exits 1 on findings
duckle-runner catalog diff main                                   # what this branch does to the graph
duckle-runner catalog build                                       # scan every pipeline
duckle-runner catalog assets                                      # every table, file, topic and endpoint
duckle-runner catalog impact postgres://db:5432/sales.public.orders
duckle-runner catalog orphans                                     # written here, read by nobody
duckle-runner catalog owners                                      # what nobody has claimed
```

`impact` is the blast radius: the pipelines that read an asset, the assets they write, everything downstream of those, and how many hops away each is. Assets that could not be named are **counted on every answer** rather than dropped, so a partial graph never looks complete. An asset name nothing in the workspace uses exits non-zero, including under `--json`, so a mistyped name in a CI gate fails instead of reporting an empty blast radius.

`build` walks the whole workspace, skipping Duckle's own folders (`runs`, `logs`, `connections`, `.duckle`), so pipelines kept in subfolders are included. The saved graph records what it was built from, so it knows when the pipelines have moved on: the CLI rebuilds on read rather than answering from a stale graph, and the console says "pipelines have changed since this was built" instead of quietly presenting an old blast radius as current. The console reports it rather than rebuilding, because reading the catalog is a viewer action and rebuilding writes a file - **Rescan** is the operator's button. The check is `stat` only, so it costs nothing to make on every read; a same-length edit inside the same millisecond would slip through, which is the price of not hashing every pipeline on every read. Asset names never carry a credential: a `mongodb://user:pass@host` uri or an ODBC connection string is reduced to the address before it becomes a name, which also keeps the name stable when the password is rotated.

Add `<workspace>/owners.json` and it also tells you who to notify. Rules are globs and the **first match wins**, so a narrow rule above a broad one carves out an exception:

```json
{
  "assets": [
    { "match": "/lake/raw/pii_*", "owner": "Privacy Office", "contact": "privacy@example.com",
      "description": "Landing zone for regulated source tables.", "tags": ["raw", "pii"] },
    { "match": "/lake/raw/*",     "owner": "Data Platform",  "contact": "data@example.com" }
  ],
  "pipelines": [{ "match": "*-ingest-*", "owner": "Ingest Squad" }],
  "terms": { "active customer": "Ordered in the last 90 days." }
}
```

The same file carries the human half of the catalog: an optional `description`
and `tags` per rule, and a workspace `terms` glossary for the words three teams
would otherwise each define differently. Every one of those is optional, so an
`owners.json` written before they existed still loads unchanged. They live here
rather than in a file of their own because they are authored, reviewed and
committed alongside ownership, and a second file would drift from this one.

Every run records **which assets it read and wrote**, with row counts, under the
same names the graph uses - so the catalog can answer the first question anyone
actually asks of an entry: *is this current?* A table nobody has written for
three weeks is the interesting one, and no amount of structure reveals that.
Only successful runs count towards freshness: a failed run may have written
nothing, or half of something, and showing either as the last write would make
a broken load look like a fresh table.

Assets also carry the **columns** the pipelines declare, unioned across every
node that touches them - a pipeline reading three columns of a table another
writes twenty to does not make the table three columns wide. They come from the
schema a node already carries, so building the graph still opens no source and
needs no credentials. No declared columns means none are *known*, which the
catalog does not confuse with the asset having none.

In the desktop app this is the **Data Catalog** screen (Home -> Govern): search
every asset by name, owner, tag or column; see who writes it, who reads it, what
columns are declared and when it was last written; and set the owner,
description and tags without leaving the app. Saving writes a rule for that
exact name **above** any wildcard covering it, so describing one file never
re-describes its neighbours. **Read live schema** opens the source on demand
through a node that already reads it, so it authenticates the way the pipeline
does - it is never done just because a screen was opened.

`catalog lint` is the gate for a CI job: it exits 1 when it finds something.
It reports ownership rules that match nothing - almost always a typo or a
renamed asset, and a failure that is otherwise silent, because the team the
rule names simply never gets told about anything - patterns that will not
compile (they own nothing, safely and invisibly), and nodes the graph could not
name. Unowned assets are reported but only fail under `--strict`: most
workspaces have a long tail nobody will ever claim, and failing CI over it on
day one is how a useful check gets deleted from the pipeline instead of acted
on.

`catalog diff <rev>` answers what a change does to the graph, which is the
question a review of a data platform actually asks: which assets appear, which
disappear, and - the one that matters most - which are still there but have
lost every pipeline that **wrote** them. A deleted asset is loud, because
something errors. An asset nothing writes any more is silent: no error, no
missing file, the table simply stops moving and whoever reads it finds out
weeks later. The revision is read straight from git's object store, so nothing
is checked out and it is safe to run on a dirty worktree.

The console's Catalog view now shows the same facts as the desktop screen -
description, tags, columns and freshness - because both are assembled by one
function in the engine rather than two that would drift. The same answers are
available over MCP as `workspace_impact`.

### Alerting (tell someone when a run fails)

`snk.email` and `snk.rest` are pipeline *nodes* - they need wiring into every pipeline and cannot fire when a pipeline dies before reaching them. `<workspace>/alerts.json` watches the runs themselves, for both the desktop scheduler and the server:

```json
{
  "rules": [
    { "match": "nightly-*", "channel": "webhook", "url": "${ENV:SLACK_WEBHOOK}", "cooldownMinutes": 15 },
    { "match": "*", "channel": "email", "smtpHost": "smtp.example.com",
      "from": "duckle@example.com", "to": ["oncall@example.com"] }
  ]
}
```

The webhook payload carries a `text` field as well as structured fields, so Slack, Teams and Discord render it directly. Three behaviours are deliberate:

- **Repeat suppression.** A five-minute schedule that starts failing would otherwise send 288 messages a day. `cooldownMinutes` (default 15) bounds it per pipeline and event.
- **The all-clear.** A success after a failure is its own `recovery` event and **ignores the cooldown**, so nobody is left thinking an outage is still running. Ordinary successes are silent unless you add `"on": ["success"]`.
- **It never breaks a run.** Delivery happens after the run is recorded, is time-bounded, and an unreachable channel is logged rather than raised.

A schedule whose pipeline file has been renamed or deleted also raises an alert, instead of silently doing nothing.

---

## Schedules and triggers

Pipelines can run on cron, fixed interval, or file-watch triggers. Configure these in the **Schedule panel** (toolbar -> Schedule icon), not as graph nodes.

| Trigger type | Config | Example |
|---|---|---|
| **Cron** | Standard 5-field cron expression with optional timezone | `0 2 * * *` (every day at 2 AM) |
| **Interval** | `every N {seconds, minutes, hours, days}` | `every 15 minutes` |
| **File watch** | Watch a directory for new/changed files matching a glob | `/inbox/*.csv` |
| **Manual** | Run-on-demand only (the default) | - |

Schedules persist to `workspace/schedules.json` and execute via the in-process scheduler crate. They survive app restarts but require Duckle to be running.

For headless / always-on schedules that run when Duckle is closed, build the pipeline into a standalone file and let the operating system's own scheduler run it - see [Server deployment](#server-deployment-build-pipeline) below.

---

## Meet Duckie - the local AI pipeline assistant

> Describe what you need. Duckie writes the pipeline.

<p align="center">
<img src="docs/assets/real-life-screenshot/duckie.png" alt="Duckie AI Assistant panel open beside a real pipeline on the canvas, showing example prompts and a LOCAL badge" width="100%"/>
</p>

The sidebar on the right is **Duckie AI Assistant** - powered by **Qwen 2.5 Coder 1.5B** running through **llama.cpp**, downloaded once (~1.1 GB) and then run entirely on your CPU. Ask in plain English; Duckie streams back a valid Duckle pipeline definition. One click drops it onto the canvas, ready to inspect, tweak, and run.

| | |
|---|---|
| **Truly local** | The Qwen model runs as a `llama-server` subprocess on `127.0.0.1`. No API keys. No network calls. Disconnect your wifi and it keeps working. |
| **Streamed responses** | Tokens arrive as they're generated, with a blinking caret in the bubble. No "wait 20 seconds for the spinner to vanish" UX. |
| **One-click insert** | When Duckie produces a JSON pipeline, an **Insert into canvas** button appears. The graph populates with positioned nodes, wired edges, and the props the model chose. |
| **Bring-your-own-model option** | The chat plumbing is the same OpenAI-compatible HTTP interface used by `xf.ai.llm` / `xf.ai.embed` connectors. Point `baseUrl` at Ollama, llama.cpp, Cohere, OpenAI, Voyage - anything that speaks the OpenAI shape. |
| **Sandboxed** | The model has no fs / net / tool access. It can only emit text - your pipeline JSON. |

---

## Benchmark

### 20M-row CSV into DuckDB

The most common job in data engineering: load a **20M-row CSV into DuckDB**. One identical 2.49 GB file (20M rows of TPC-H lineitem, 16 typed columns), every tool measured at its best configuration, wall-clock time to land the data as a table.

<p align="center">
  <a href="https://duckle.org/"><img src="docs/assets/ingest-seconds-benchmark.png" alt="Benchmark: loading a 20M-row CSV into DuckDB at each tool's best config. Duckle 15.69s, dlt 40.68s, Talend bulk 90s, Informatica bulk 100s, ingestr 411s, Airbyte about 1150s." width="820"/></a>
</p>

**How it was measured**

- **Machine:** Intel Core i7-13650HX (14C / 20T), 24 GB RAM, NVMe SSD, Windows 11, DuckDB 1.5.4. Duckle, dlt and ingestr ran here.
- **Best config per tool:** dlt used the Arrow plus parquet loader path; Talend and Informatica used their bulk output connectors at max config (their default row-by-row sinks were 5-7x slower). Nothing was left on a slow default to pad the gap.
- **Talend and Informatica** ran on a separate 8 GB VPS, so per-tool peak RAM was not captured for those two.
- **Airbyte** (source-file to destination-duckdb) is scaled from measured 2M and 5M runs at a steady ~18k rows/s, and it also needs an always-on ~8 GB platform just to start.
- Wall-clock time, peak working-set of the whole process tree.

**Why Duckle is this fast:** its 15.69s sits right on top of raw DuckDB's own load floor (~16s to fully parse and write all 20M typed rows into an on-disk table). Duckle wraps the engine with pipelines, connectors, and a UI, then gets out of its way. That is the entire design goal. A read-only scan or aggregate over the same CSV is far faster still; this benchmark measures the heavier "materialize it as a table" job that every ETL tool here performs.

### 96M rows, Postgres to Parquet

A second, harder job against a live database: full-refresh extract of **95,988,640 rows** of TPC-H `lineitem` (14 GB in Postgres 16) out to Parquet.

<p align="center">
  <img src="docs/assets/pg-to-parquet-benchmark.png" alt="Benchmark: 96M rows Postgres to Parquet. Duckle 39.9s, raw DuckDB postgres_scanner floor 44.2s, ingestr 120.8s, dlt 493.6s, sling 1897s." width="880"/>
</p>

**Run it yourself.** The harness is in this repo at [`benchmarks/pg-to-parquet`](benchmarks/pg-to-parquet). `./bench.sh all` brings up Postgres, generates the data at any scale factor, and times every tool you have installed. No timing is recorded until the output has been reopened and checked for the right row count and the right `sum(l_orderkey)`, so a tool that writes a fast but wrong file gets a failure rather than a number.

**Read it with these caveats**

- **The DuckDB floor is not a competitor.** It is raw `postgres_scanner` plus `COPY TO`: no scheduling, no typing, no incremental state, no UI. It is there to show how much of the clock is the machine reading Postgres. Duckle landing 11% under it is the honest framing, not "Duckle beats DuckDB".
- **ingestr has no Parquet destination** and writes a DuckDB file, so its output size is not like-for-like. Its time is.
- **Compression is not normalised.** Duckle wrote zstd, the others snappy.
- **Airbyte and Meltano are absent.** Airbyte has no local Parquet destination and has only run against an earlier synthetic dataset; Meltano was not wired up. Neither is claimed here.

Hardware, per-run numbers and the two measurement traps that produced wrong figures on the first attempt are written up in [`RESULTS.md`](benchmarks/pg-to-parquet/RESULTS.md).

---

## Status

Duckle is in **public beta**. The visual designer, the DuckDB execution engine, the scheduler, the cloud connectors, and the Duckie AI assistant all work today and are covered by 170+ integration tests across Linux, macOS, and Windows. The catalog is still growing and APIs may evolve before 1.0, but the day-to-day surface is stable enough for real work.

**Scope, stated plainly:** Duckle runs as a service on hardware you provision, and uses all of it. What it does not do is split one query across a cluster, so when a job outgrows the largest instance you want to pay for, push the work down into the source system or point the output at a warehouse, object store or lakehouse. It will not pretend to be a cluster.

The component palette ships **384 nodes** so the roadmap is visible in the product itself:

- **366 available** runs on the DuckDB engine today
- **3 preview** is configurable in the designer (drag, wire, set properties); execution is being wired engine-by-engine
- **15 planned** is reserved in the palette but not yet executable - see [`docs/roadmap.md`](docs/roadmap.md)

---

## Capabilities

Duckle is not a CSV tool with extras. It reads a broad set of formats and sources, ships a deep transform library, and writes to files, databases, object storage, vector DBs, message buses, and email.

### Sources

**113 sources available today.**

| Group | Connectors | Status |
|---|---|---|
| **Files** | CSV, TSV, Parquet, JSON, JSONL / NDJSON, Excel (.xlsx), YAML, TOML, Fixed-width (mainframe / banking positional dumps), XML (slash-separated rowPath), Apache Avro (.avro / .ocf, pure-Rust) | Available |
| **Geospatial files** | GeoJSON, Shapefile, GeoPackage, KML, GPX, GML via the `spatial` extension | Available (lazy-loaded) |
| **File Geodatabase** | Esri File Geodatabase (`.gdb`) feature classes via `ST_Read` with a per-layer selector | Available (lazy-loaded) |
| **Hugging Face** | Hugging Face Hub datasets over `hf://` (Parquet / CSV / JSON, globs, revisions); token for private or gated datasets | Available |
| **Geospatial** | Read GeoJSON / Shapefile / GeoPackage / KML / GPX / Esri File Geodatabase; write those plus **GeoParquet**; CRS-aware measurement, reprojection, spatial joins and predicates | Available |
| **Lakehouse table formats** | Apache Iceberg, Delta Lake, DuckLake (catalog in a local file or a `postgres:` / `mysql:` / `sqlite:` DSN, with the catalog schema and `META_*` parameters - including `META_SECRET` - settable on the node) | Available |
| **Embedded databases** | SQLite (read tables), DuckDB (read tables or run a query) | Available |
| **Network relational DBs** | PostgreSQL, MySQL, MariaDB, CockroachDB | Available (live CI for PG + MySQL) |
| **Network relational DBs** | SQL Server (TDS), Oracle (Instant Client at runtime), ClickHouse (HTTP API), **IBM DB2** (IBM Data Server ODBC driver), **Turso / libSQL** (HTTP pipeline API - no driver install; `libsql://` URLs accepted) | Available |
| **Network relational DBs** | generic JDBC | Planned |
| **Object storage** | Amazon S3, Google Cloud Storage, Azure Blob, HTTP(S), MinIO, Cloudflare R2, Backblaze B2 | Available (live CI for MinIO) |
| **Cloud warehouses** | MotherDuck, Snowflake (SQL API + PAT/JWT), BigQuery, Redshift (postgres ATTACH), Databricks SQL (Statement Execution + chunk follow), Azure Synapse (TDS), **Teradata** (ODBC, Windows / Linux), **DuckDB Quack** (May 2026 remote protocol - HTTP on :9494, SECRET-based token auth) | Available |
| **Streaming** | Apache Kafka / Redpanda (pure-Rust `rskafka`), NATS JetStream, GCP Pub/Sub (REST + auto-ack), RabbitMQ (`lapin` AMQP), AWS Kinesis (HTTP + SigV4 - no AWS SDK), WebSocket (`ws://` / `wss://`, optional subscribe frame) | Available |
| **Streaming** | Pulsar, Event Hubs, multi-shard Kinesis | Planned |
| **APIs and SaaS (REST)** | Salesforce, HubSpot, Pipedrive, Zendesk, Intercom, Stripe, QuickBooks, Xero, Shopify, Notion, Airtable, Asana, Trello, ClickUp, Monday.com, GitHub, GitLab, Linear, Jira, Slack, Discord, Telegram, Twilio, Mailchimp, SendGrid, Segment - thin pre-configured wrappers over `src.rest` / `src.graphql`. `src.rest` takes a configurable API-key auth header name and offset pagination that stops on a body `total_count`. **Salesforce Bulk** (`src.salesforce.bulk`) - Bulk API 2.0 query source for migration-scale reads: SOQL as an async query job (query / queryAll), paged CSV result sets streamed to disk via `Sforce-Locator`, typed empty relations on 0 records | Available |
| **APIs (protocols)** | OData v4 (follows `@odata.nextLink`), SOAP / generic XML APIs (XML response parsing with namespace local-name match) | Available |
| **Health data (DHIS2)** | `src.dhis2` reads the DHIS2 Web API: aggregate `dataValueSets`, paged metadata lists, tracker exports, and `analytics/dataValueSet.json`. `snk.dhis2` imports back: chunked requests, `importStrategy` (CREATE_AND_UPDATE is DHIS2's upsert), `dryRun`, and real import-summary parsing, so conflicts and a non-zero `ignored` count fail the run instead of passing as a green HTTP 200. Auth via personal access token or HTTP Basic. Raw `/api/analytics` (columnar `headers[]` + `rows[][]`) is not supported | Available |
| **NoSQL and search** | **Neo4j** (Cypher over the HTTP Query API - self-hosted or Aura, no Bolt driver; optional `$parameters`), MongoDB (official driver), Cassandra / ScyllaDB (CQL), Elasticsearch / OpenSearch (from+size + search_after), Redis (SCAN + GET), CouchDB (`_all_docs`), DynamoDB (HTTP + SigV4 - no AWS SDK; auto-unwraps typed attributes) | Available |
| **Vector / AI databases** | pgvector (postgres ATTACH), Qdrant (`/points/scroll`), Weaviate (`/v1/objects`), Milvus (`/v1/vector/query`) | Available |
| **Vector / AI databases** | Pinecone (no list-all-vectors API), Chroma, LanceDB | Preview |
| **File transfer** | FTP / FTPS (pure-Rust `suppaftp`) and SFTP (SSH, pure-Rust `russh` + `russh-sftp` on the ring backend; password or private-key auth) - one File Transfer component, pick the protocol. Glob filter, base64 content per file. **Host keys are verified**: pin a SHA256 fingerprint to accept only that key, or leave it empty and the first key seen for a host is recorded in `<workspace>/.duckle/known_hosts`, after which a different key is refused. A host that presents an OpenSSH certificate is accepted only when it certifies the key you pinned. `DUCKLE_SFTP_HOST_KEY_POLICY=accept-any` opts out for a host whose key changes per connection | Available |
| **Mailbox** | IMAP (rustls TLS, `mail-parser`) - basic auth today, OAuth (gmail / o365) on the roadmap | Available |
| **Webhook listener** | Binds `127.0.0.1:port`, collects N inbound HTTP requests with a timeout, parses JSON-object / JSON-array bodies into rows | Available |
| **Desktop** | System clipboard (pure-Rust `arboard`, auto-detects JSON-array shape) | Available |
| **Repos** | Git (commit log or file tree from a local working copy; shells out to system `git` CLI) | Available |

For CSV / TSV sources, the **Schema** panel accepts an optional per-column **Format** (a `strptime` token string such as `%d/%m/%Y`) on Date and Timestamp columns. Several date columns can each parse a different layout in one read - the column is read as text and re-parsed with its own format, working around DuckDB's single global date format. A value that does not match its format becomes null rather than failing the run. Set a Date or Timestamp column's Format to `excel` to convert Excel day-serials correctly. CSV sources also surface `ignoreErrors` (skip unparseable rows) and `nullPadding` (pad short rows with nulls) toggles in the GUI.

For JSON sources, a **Format** selector picks how the file is read (auto / array / JSON Lines / object), and a **skip malformed records** toggle drops records that fail to parse instead of failing the run.

### Transforms

**130 transforms available today.**

| Group | Operations |
|---|---|
| **Fields** | Map (visual mapper: joins a main input to up to 3 lookup inputs with inner / left joins and per-output expressions + filter), Project / Select, Cast, Rename, Add / Drop / Reorder Column, Coalesce, UUID v4 |
| **Rows** | Filter (visual or raw SQL, with reject port), Distinct, Sample, Top N / Limit, Sort, Skip, Top N per Group, Forward Fill, Backward Fill, Constant Fill |
| **Aggregate** | Group By, Rollup, Cube, Count, Window Aggregate, Cumulative, Approx Quantile (t-digest), Approx Count Distinct (HyperLogLog) |
| **Join** | Inner, Left, Right, Full Outer, Cross, Lookup, Semi, Anti, Spatial Join (Intersects, Contains, Within, Touches, Crosses, Overlaps, Equals, Covers, Covered by; fails naming both systems when the two geometry columns use different CRS, rather than returning zero rows) |
| **Set operations** | Union, Union All, Intersect, Except / Minus |
| **Window** | Row Number, Rank, Dense Rank, Lead, Lag, First Value, Last Value, NTile |
| **Strings** | Regex Replace, Regex Extract, Regex Match, Split, Concat, Trim, Case Change, Length, Substring, Format, Hash (md5 / sha1 / sha256), IP Parse, URL Parse, Text Similarity (Levenshtein / Jaro-Winkler / Jaccard), Base64, Pad, Text Match |
| **Date / Time** | Parse, Format, Extract Part, Date Diff / Add, Truncate, Timezone Convert, Time Bin, Current Timestamp, Epoch Convert |
| **Numeric** | Round, Modulo, Absolute, Logarithm, Power, Square Root, Bucketize, Z-Score, Clamp, Sign |
| **JSON / nested** | Parse, Stringify, Flatten, JSONPath Extract, Merge Objects, Array Aggregate, jq Filter (a jq program per row over a JSON column, run in-process by the pure-Rust jaq engine - no external jq, no subprocess) |
| **Array** | Explode / Unnest, Collect List, Element At, Contains, Distinct, Length, Zip Arrays to Table (headings + row-arrays -> one column per heading) |
| **Pivot / shape** | Pivot, Unpivot, Denormalize, Normalize, Transpose |
| **Quality gate** | Every check offers On failure: **reject** (route the bad rows to the reject port, the default), **warn**, or **fail** (stop the run). `fail` raises where the rows are counted, so a gate asked to stop a load stops it |
| **CDC / SCD** | Incremental Load (watermark column; saves the high-water mark to workspace state and advances only on a fully successful run), Diff Detect, SCD Type 1, SCD Type 2 (valid_from / valid_to / is_current), Merge / Upsert (universal across embedded, network, warehouse and Mongo sinks, with optional delete propagation driven by a CDC change-type column), DuckLake CDC change-feed reader, Row Hash (md5 / sha1 / sha256 fingerprint), Audit Stamp (`_loaded_at` / `_loaded_date` / `_source` / `_batch_id`) |
| **AI / Search** | **Vector Similarity Search** (cosine / L2 / inner product over FLOAT[N] via `vss`), **Full-Text Search** (BM25 via `fts`), **Embeddings** (OpenAI-compatible `/v1/embeddings`), **LLM Transform** (per-row chat completion with `{column}` templates), **Classify** (LLM-backed, normalizes to UNKNOWN), **Text Chunker** (RAG-ready, pure local), **PII Redact** (regex - emails / phones / SSNs / cards), **Semantic Dedupe** (cosine over precomputed embeddings) |
| **Geospatial** | Spatial Distance, Length, Perimeter, Area (each auto-picks the planar or spheroidal function from the geometry CRS, and rejects geometry with no CRS), Spatial Buffer (ST_Buffer), Spatial Intersects (ST_Intersects), Flip Coordinates (ST_FlipCoordinates - fix lat,lon vs lon,lat order), Define Projection (ST_SetCRS - stamp a CRS without moving coordinates), Reproject Geometry (ST_Transform between CRS, target CRS preserved on the output), Create Geometry (from X/Y, WKT, or WKB), Clip Geometry and Erase Geometry (two-layer overlays; the second layer is dissolved with ST_Union_Agg so a feature spanning several polygons is not duplicated, and both refuse to run when the two layers carry different CRS) |
| **Debug** | Log Rows, Assert (hard-fail on SQL predicate violation) |

> **All 6 AI transforms ship today.** Three need a model API (LLM, Classify, Embeddings) and ride the apiKey-in-props pattern; three are pure-local (Chunk, PII Redact, Dedupe).

### Data quality

**27 validators available today.**

Validators split their input: passing rows continue on the main port, failures route to a **reject** port you can sink, count, or inspect.

| Component | Behavior |
|---|---|
| **Not-Null Check** | Pass rows with no nulls in the chosen columns |
| **Range Check** | Pass rows inside a numeric range (inclusive or exclusive) |
| **Regex Match** | Pass rows whose column fully matches a pattern |
| **Uniqueness Check** | Pass the first row per key; route duplicates to reject |
| **Schema Validate** | Reject rows where any expected column is null |
| **Column Profile** | Per-column stats (count, null %, distinct, min / max, quartiles) via `SUMMARIZE` |
| **Describe** | Column names + types of the input |
| **Histogram** | Value frequencies for one column, most-frequent first |
| **Standardize** | Trim + case-normalize + collapse inner whitespace, in place |
| **Fuzzy Deduplicate** | Keep the first row per near-duplicate cluster |
| **Record Match** | Self-join: emit pairs of rows above a similarity threshold |
| **Expectation Suite** | A reusable suite of rules plus a data-quality scorecard, so one node carries the whole expectation set |
| **Data Contract** | Enforcement gate holding the same rule suite, failing the run when the contract is broken |
| **Freshness / SLA** | Age is now minus the newest value in a column, checked against a maximum you set |
| **Outlier Detection** | Statistical outlier detection; inliers continue, outliers route to reject |
| **Referential Integrity** | Orphan check across two inputs: rows whose key is absent from the reference route to reject |
| **Reconciliation** | Source-versus-target report, for proving a load matches what it came from |
| **Record Linkage** | Fuzzy linkage across two inputs, matching records that are not identical |
| **Match Grouping** | Turns matched record pairs into stable clusters, so a chain of matches becomes one group |
| **Survivorship** | Collapses duplicates sharing a group key into a single surviving record |
| **Mask / Anonymize** | Irreversibly masks or anonymizes selected columns in place |
| **Column Classification** | Heuristic column classification and PII tagging. No LLM, no data leaves the machine |
| **Advanced Column Profile** | A richer single-column profile than Describe |
| **Reproducible Sample** | A repeatable random sample of the upstream rows |
| **Validate Geometry** | Flags invalid geometries with `ST_IsValid` |
| **Repair Geometry** | Replaces the geometry column in place with a repaired one |
| **Check Empty Geometry** | Flags empty geometries with `ST_IsEmpty` |
| **Address Cleanse** | Address parsing / normalization (planned - needs external lib) |

### Custom code

**7 ways to drop into code available today.**

| Capability | What it does |
|---|---|
| **Inline SQL** | Write a `SELECT`; the upstream node is exposed as `input`, result runs as a real materialized stage. A **raw SQL** mode runs verbatim SQL (a leading `WITH` / multiple CTEs / UNIONs) with no input-CTE wrapper |
| **SQL Template** | Parameterized SQL with `${context.var}` substitution |
| **SQL Routines** | Reusable, named SQL saved in the workspace |
| **dbt** | Run a dbt project (or one inline model) as a node, against the pipeline's DuckDB. Wire several upstream sources in and the project reads them all via dbt `sources`, so one project models across Postgres, MySQL, files, and lakes at once. Powered by the dbt Fusion engine, fetched free at first launch (Apache dbt-core fallback); no Python setup. |
| **Shell** | Run any shell command; emits `{stdout, stderr, exit_code, duration_ms}`. Platform-aware default shell. Optional `timeoutMs` kills the child. |
| **WebAssembly UDF** | Per-row WASM transform via pure-Rust `wasmi`. Sandboxed (no fs / net / env). Works with any WASM toolchain (Rust, AssemblyScript, C, TinyGo). |
| **JavaScript UDF** | Per-row JS transform via pure-Rust `boa` interpreter. Sandboxed. Define a `transform(row)` function. |
| **Python / Rust UDFs** | Embedded-language stages | Planned |

### Sinks

**73 sinks available today.**

| Group | Connectors | Status |
|---|---|---|
| **Files** | CSV, TSV, Parquet (ZSTD), JSON, JSONL / NDJSON, Excel (.xlsx), YAML, TOML, XML (configurable wrappers), Avro (schema inferred from first row). Parquet + CSV support Hive-partitioned writes | Available |
| **Geospatial files** | GeoJSON, GeoPackage, Shapefile, KML, GPX via GDAL | Available (lazy-loaded) |
| **Lakehouse** | Apache Iceberg (full table layout), DuckLake - modes: **overwrite**, **append**, **truncate**, **upsert** (set-based delete-by-key + re-insert), **merge** (partial-column `MERGE INTO` that preserves columns the source omits) with optional CDC delete propagation, plus **publish groups** - several DuckLake sinks sharing a group name commit as one snapshot, so readers see all of their tables update together or none of them, and a run that cannot honour the group is refused rather than publishing part of it | Available |
| **Embedded databases** | SQLite, DuckDB - modes: **overwrite**, **append**, **upsert** (set-based delete-by-key + re-insert, no PK required), **merge** (partial-column `MERGE INTO` that preserves columns the source omits) with optional CDC delete propagation | Available |
| **Network relational DBs** | PostgreSQL, MySQL, MariaDB, CockroachDB - modes: **overwrite**, **append**, **truncate**, **upsert** (ON CONFLICT / ON DUPLICATE KEY) with optional CDC delete propagation | Available (live CI for PG + MySQL) |
| **Network relational DBs** | SQL Server / Azure Synapse (TDS, multi-row VALUES batched; auto-creates the table if absent; **upsert** via MERGE), Oracle (Instant Client; INSERT ALL, batched per statement; auto-creates the table if absent; **upsert** via MERGE), ClickHouse (HTTP JSONEachRow; upsert by pointing at a ReplacingMergeTree target table), **IBM DB2** (ODBC; auto-creates the table, booleans as SMALLINT 1/0 so DB2 for z/OS also accepts them), **Turso / libSQL** (HTTP pipeline API; auto-creates the table, values sent as bound parameters) - every MERGE sink supports **CDC delete propagation** (a delete-flag column removes matched rows) | Available (SQL Server + Oracle + MySQL upsert and delete propagation verified live in Docker) |
| **Network relational DBs** | generic JDBC | Planned |
| **Object storage** | S3, GCS, Azure Blob via DuckDB `httpfs` (MinIO / R2 / B2 via endpoint) | Available |
| **Hugging Face** | Push to a Hugging Face Hub dataset repo (`snk.huggingface`): the upstream is materialized to Parquet and committed over the Hub API (create-repo → preupload → git-LFS → commit); write token required, repo auto-created (public or private) | Available |
| **Cloud warehouses** | MotherDuck, Snowflake (PAT or JWT RS256; **upsert** + delete propagation via MERGE), BigQuery, Redshift, Databricks SQL (**upsert** + delete propagation via MERGE), Azure Synapse, **Teradata** (ODBC), **DuckDB Quack** (concurrent writers to remote DuckDB via the May 2026 protocol) | Available (Snowflake MERGE verified live against the SQL-API emulator) |
| **HTTP APIs** | REST (POST/PUT/PATCH batched JSON-array; configurable API-key auth header name), Webhook (one POST per row), GraphQL mutations | Available |
| **SaaS / CRM** | Salesforce (`snk.salesforce`) - sObject Collections API: **insert / update / upsert (by external Id) / delete**, ≤200 records/request, Bearer token or OAuth 2.0 client-credentials (fresh token minted per run, same auth as `src.salesforce`). **Salesforce Bulk** (`snk.salesforce.bulk`) - Bulk API 2.0 for migration-scale loads: **insert / update / upsert / delete / hardDelete**, DuckDB streams to CSV and each ≤90 MB part runs as an async job | Available |
| **Email (SMTP)** | Per-row SMTP send via pure-Rust `lettre` + rustls. Plain text v1; HTML + attachments follow. | Available |
| **NoSQL** | **Neo4j** (rows as nodes over the HTTP Query API; one `UNWIND $rows` round trip per batch, `mergeKeys` switches CREATE to MERGE so re-runs update rather than duplicate), MongoDB (insert_many batched; **upsert** via replace_one on a key, plus delete propagation via delete_one), Cassandra / ScyllaDB (CQL), Elasticsearch / OpenSearch (`_bulk` NDJSON), Redis (pipelined SET) | Available |
| **NoSQL** | DynamoDB | Planned |
| **Streaming** | Kafka / Redpanda (`rskafka`), NATS JetStream, GCP Pub/Sub (REST + OAuth2), RabbitMQ (`lapin`), WebSocket (`ws://` / `wss://`) | Available |
| **Streaming** | Pulsar, Kinesis | Planned |
| **Vector / AI databases** | pgvector, Pinecone (`/vectors/upsert`), Qdrant (`/points` PUT), Weaviate (`/v1/batch/objects`), Milvus (`/v1/vector/insert`) | Available |
| **Vector / AI databases** | Chroma, LanceDB | Preview (need vendor SDK) |

Database sinks support an optional **dead-letter (validate-before-insert)** step: rows that do not match the declared column types are split off to a dead-letter file (parquet / csv / json) and only the clean rows are inserted.

### Control flow

**18 control-flow components available today.**

| Component | What it does |
|---|---|
| **Replicate / Tee** | Send the same data to multiple downstream outputs |
| **Merge Streams** | Concatenate multiple input streams (UNION ALL) |
| **Switch / Conditional Split** | Route rows to `case_1..N` outputs by boolean (first match wins); `default` for unmatched |
| **Wait / Delay** | Sleep `N ms / s / min / h` before passing rows through |
| **Throttle** | Inter-stage delay derived from a rows-per-second target |
| **Set Run Variable** | Work out a value while the run is under way and let later steps read it as `${name}` (`ctl.setvar`), in this pipeline and in the jobs it goes on to run. Wired to rows the expression is read against them and the first row decides, so use an aggregate to read the whole input; wired to nothing it stands on its own. The value is held in the run's own database, so it survives whichever way the engine executes the stages |
| **Checkpoint** | Pass rows through and also write a parquet snapshot to a path |
| **Dead Letter Queue** | Terminal sink for rejected rows (JSON / CSV / Parquet) |
| **Run Pipeline** | Inline-execute another pipeline file (`ctl.runpipeline`) |
| **Run Job** | Call a child pipeline (picked from the workspace) passing parent context variables; chain several to build a Master Job (`ctl.runjob`). The child runs for its side effects: it gets its own temporary database and its output is not composed back into the parent, so a child cannot yet return rows to its caller |
| **Parallelize** | Run the downstream branches wired to its outputs concurrently; branches are unlimited (`ctl.parallelize`) |
| **Iterate** | Run a sub-pipeline N times with `${ITER_INDEX}` substitution |
| **For Each** | Run a sub-pipeline once per input row with `${ITER_ITEM_<FIELD>}` substitution; an optional item key column names each run so per-row watermarks stay separate |

| **Try / Catch** | Install a fallback sub-pipeline if the wrapped stage fails |
| **Retry** | Per-stage retry policy (configure on Advanced tab) |
| **Log Message** | Emit an info log line (`{rows}` = upstream count), pass rows through (`ctl.log`) |
| **Warn** | Emit a warning log line, pass rows through (`ctl.warn`) |
| **Die / Fail** | Stop the run with a message: always, only when the input has rows, or only when empty (`ctl.die`) |
| **Schedule** | Cron / interval / file-watch triggers via the orchestration crate |

A run variable is read as a value wherever the SQL of a later step names it: on its
own, as a whole string literal (`'${name}'`, the usual way to write a value into a
`WHERE` clause, where the quotes come off with it), or inside a longer literal, which
is joined around it. A name a node sets this way is left for the run to fill in, so a
static context entry of the same name does not pre-empt it.

It also travels into whatever the pipeline runs. A Run Job, an Iterate or a For Each
started after the value was worked out hands it to the child as `${name}`, and on again
to whatever that child runs, so a value settled in a master job reaches a body several
levels down. A value named on the call itself still wins, since naming one there is how
a parent says which value to run the child with. A name whose value came out NULL is
not passed: it has no value, and an unset `${...}` is left as it is rather than arriving
as the word NULL.

A sub-pipeline runs under its own name, so its run log lands in
`logs/<child>/` and an `xf.incremental` watermark inside it is saved to
`state/<child>/<node>.json`. Two different children driven by the same For Each
therefore keep separate marks.

Set **For Each -> Item key column** to separate the ITERATIONS too. The child
then runs as `<child>@<value>`, so loading 400 tables through one sub-pipeline
keeps 400 watermarks in `state/<child>@<table>/<node>.json` instead of one.
Leave it blank and every row shares a single mark, which silently skips rows
when each row is a different table. It is never inferred from the row's
position, because that would move every watermark the moment the driving query
is reordered.

Set **For Each -> Dispatch** to *Queue for workers* and the rows are written to
`batches/<id>.ndjson` instead of being run, one JSON line per row carrying the
child reference and that row's substitutions. Nothing runs until a worker picks
the batch up, so the run that queued it reports how many items are waiting
rather than pretending they loaded. A batch is a file in the workspace like
everything else here: no queue server, no database, no network service.

Queueing also reports whether the items are actually safe to spread out. Both
"400 items each loading their own table" and "400 items appending to one file"
look identical on the canvas - one sink node with a variable in the path - and
only the first survives being run at once. So each item's variables are put
into the child and the resulting targets are named with the same function that
builds the workspace catalog, before anything picks the batch up:

```
duckle: 400 item(s) write to targets nothing else in the batch writes, so they
        are safe to run at the same time
duckle: heads up - 1 target(s) are written by more than one item (400 items
        write /lake/everything.parquet). Workers run items at the same time, so
        these will collide unless the sink is an upsert or the target is
        append-safe
```

It warns rather than refuses: appending many items into one table is a real
thing to want, and only you know whether that sink is safe for it. Items whose
child cannot be read are counted and reported, so a partial check never reads
as a clean one.

Then run workers against it:

```bash
duckle-runner work --workspace /path/to/workspace     # drain every batch
duckle-runner work --batch fe-20260816T101112123      # just this one
duckle-runner work --once                             # one item, then exit

duckle-runner work status                             # what is stuck, and why
duckle-runner work retry --dead                       # start the stuck ones over
```

Start it on several machines pointed at one workspace and they share the batch.
Each item is claimed with the same OS lock a pipeline run uses, so no two
workers take the same one, and a worker that is killed mid-item leaves nothing
to clean up: the kernel drops the lock and the item becomes claimable again.
There is no lease, no heartbeat and no timeout, because there is nothing to
expire. Progress is appended to `batches/<id>.ledger.ndjson`, so re-running a
worker resumes rather than repeats.

**Retries are bounded.** A failed item stays claimable and is tried again on a
later pass, which is right for a timeout and wrong for a 404 that will always be
a 404: without a limit that item takes a worker slot on every pass forever. Set
**Max attempts per item** on the For Each node, with a fixed or exponential
backoff, and an item that uses them up is left alone and reported as dead rather
than chased. `work status` lists what is waiting out a backoff and what is dead,
with the last error; `work retry --dead` starts the dead ones over. A retry
appends a reset marker rather than rewriting the ledger, so the failures stay
readable - an item that died four times before someone fixed the source still
says so. Leave max attempts at 0 and behaviour is exactly what it was.

Items run **at least once, not exactly once.** The ledger is written after an
item succeeds, so a worker that finishes an item and then dies leaves it
looking undone and another worker repeats it. That is the honest trade for
having no transactional store - the alternative loses items instead of
repeating them, and a lost load is worse. Make the child idempotent (an upsert
sink rather than an append) and a repeat costs time, not correctness. A failed
item stays claimable and is retried on a later pass, with the failure kept in
the ledger.

The console has a **Batches** view: progress per batch, how many items are
running right now, how many failed, and the recent attempts with the worker
that ran each one. "Running" is answered by asking the run lock rather than by
trusting a heartbeat, so a worker that died is not counted as running and there
is no lease that could have gone stale. **Retry failed** clears the recorded
failures so those items are claimable again, keeping the successes so a retry
never repeats finished work.

Before running anything, a worker **proves the lock actually excludes on that
filesystem**: it takes a lock and asks a second process whether it can take the
same one. Some shared filesystems tell every caller it has the lock - NFS with
no lock daemon is the classic case - and on one of those every worker would
claim every item and each item would run once per worker, silently, with no
error anywhere. A worker refuses to start there. Check it yourself with
`duckle-runner work --check`; `--no-check` overrides, knowing the above. A test
that could not be *run* is only a warning, because failing to prove exclusion
is not the same as having disproved it.

Measured on one machine: three workers against a twelve-item batch took four
items each, with no item run twice. **Several machines against one shared
filesystem is the design intent and is not yet measured**, so treat it as
untested until it is. `scripts/measure-multi-host-batch.sh` is the measurement:
point it at a shared workspace and two or more hosts and it counts duplicate
executions, failing if there are any.

### Advanced settings (per-node)

Every node has an **Advanced** tab with fields the engine honours at run time:

| Field | What it does |
|---|---|
| **Retry attempts** | Total tries on failure (1 = no retry). Sleeps `backoff * attempt` ms between attempts. |
| **Retry backoff (ms)** | Inter-attempt sleep, linearly scaled by attempt index. |
| **Memory limit (MB)** | `PRAGMA memory_limit` applied to this stage only. |
| **Log row count** | Print the post-stage rowcount to the run output. |

### Orchestration and workspace

| Capability | What it does |
|---|---|
| **Run feedback** | Streaming run events light nodes up stage by stage, with per-node row counts, real mid-query cancel, and run history. |
| **Error traceback** | A failed stage reports the exact compiled SQL plus the underlying DuckDB message, in both the Run view and the NDJSON run log, so any component's failure is debuggable. |
| **Column lineage** | A top-bar **Lineage** button shows, per node, each output column traced back to the source column(s) it derives from. |
| **Dives + dashboards** | Live-querying, shareable data views that run where your data already is, stitched into multi-chart dashboards. Generate a chart from a plain-language question, export a dive to a self-contained HTML file, open standalone `/dive/<id>` and `/dash/<id>` share pages, and find everything in the top-bar **Dives** gallery. |
| **Artifacts** | `src.artifact` gives one row per file described the way a pipeline can reason about it - `uri`, `name`, `media_type`, `size_bytes`, `sha256`, `modified_at` - for PDFs, images, archives, OCR output and model binaries. An artifact is a reference, not the bytes, so it joins, filters and iterates like any other table. Hashing is off by default because it reads every byte |
| **Python, row or table** | `code.python` takes `process(row)` for a row at a time, or `transform(table)` to be handed the whole table as a pyarrow Table - for polars/pandas work, OCR, entity resolution or ML. The table path goes through Parquet rather than JSON: measured 2.11s -> 0.74s on 200k rows, and it keeps types, where the row path turns every timestamp into a string. Needs pyarrow only when `transform` is used |
| **A workspace's own Python** | A Python stage is only reproducible if the packages it needs travel with the pipeline rather than being whatever the machine happens to have. Put a virtual environment at `.venv` in the workspace - `uv venv && uv pip install pyarrow polars`, or the stdlib `python -m venv` - and `code.python` uses that interpreter on every machine, laptop, CI and headless runner alike. Nothing is installed at run time, so an air-gapped box stays air-gapped, and `DUCKLE_PYTHON_BIN` still overrides everything |
| **Batch inference that survives a rate limit** | `xf.ai.llm`, `xf.ai.classify` and `xf.ai.embed` take *Parallel requests* and *Retries on rate limit*. A 429 or 5xx is now retried per request, honouring `Retry-After`, instead of failing the stage: before this, one rate limit at row 400,000 threw away the 399,999 rows already paid for, because the only retry in the engine re-runs a whole stage from row 0. Requests run up to *Parallel requests* in flight and results are written back by index, so the output row order is still the input row order. Both default to today's behaviour (1 in flight, 3 retries). `xf.ai.llm` also finally sends *Max tokens*, a field the panel has offered since v0.5.4 while the request never carried it |
| **Blocking for entity resolution** | Every fuzzy match compares pairs, and comparing all of them grows with the product of the row counts, so linking 100k records against 100k is 10 billion comparisons. `qa.block` proposes only the pairs worth comparing: named rules of columns that must be equal (same postcode, same surname initial), each one pass, a pair caught by several rules still emitted once. One input dedupes within a table, the lookup port links two. It emits `id_a`, `id_b`, `blocking_rule` and `a_<col>`/`b_<col>` for carried columns, which is exactly what `qa.matchgroup` reads by default, so blocking, comparison with `xf.addcol`, banding with `ctl.switch` and clustering chain up out of components that already exist |
| **One REST node per parent row** | Real APIs are rarely one endpoint: `/companies` gives you ids, and the data you want is at `/companies/{id}/officers`. Give `src.rest` a *URL per upstream row* and wire a parent into its input, and it makes one request per row, substituting `{column}` from that row, unioning every result into its one output table. The shared connection, the single OAuth mint, the auth headers and all five pagination strategies are reused per request rather than re-done. *Carry upstream column* stamps the parent's key onto each child row so the two can be joined back together, and chaining three nodes main-to-main gives three real relations rather than one opaque loop. Unwired, the node is the plain source it has always been |
| **Runs that outlive the request** | A backfill can run for hours, and a synchronous HTTP call is the wrong place to keep it: clients, proxies and load balancers all time out while the pipeline is still legitimately working. `POST /api/run/async` answers `202` with a `runId` straight away; `GET /api/run/status?runId=` reports `queued`, `running` or `finished` with the pipeline's own status; `DELETE /api/run?runId=` cancels, which is polled at every stage boundary and kills the active DuckDB child so even a long query stops promptly. Every run record now carries the id it was accepted under, so a console that restarted mid-run can still answer for it. `POST /api/run` is unchanged for anything that wants to wait |
| **HTML as a source** | A great deal of public data is published only as HTML: registries, filing pages, results tables. `src.html` reads a local file or an http(s) URL and turns it into rows by CSS selector. Name a column per sub-selector (`a@href` reads an attribute), or leave the columns empty and let a table be a table: the `th` cells name the columns and each `tr` is a row. Parsed with a tolerant HTML parser, so the unclosed tags and unquoted attributes real pages carry - and that the strict XML reader rejects outright - are fine. A selector that does not parse fails the run naming it, rather than quietly producing a table of nulls |
| **HTTP transport, set once** | Proxies, timeouts and a User-Agent are transport, not credentials, and every HTTP-backed component wants the same ones. A saved **HTTP transport** connection carries a proxy, a read timeout, a connect timeout and a User-Agent, and `src.rest` and `src.html` reference it alongside their auth connection, so a corporate proxy is one edit rather than one per node. What a node sets itself still wins. Every request in the engine also now has deadlines: a connect timeout of 30s and a read timeout of 300s, both overridable with `DUCKLE_HTTP_CONNECT_TIMEOUT` and `DUCKLE_HTTP_READ_TIMEOUT`. They are per-read, not per-transfer, so streaming a large file is unaffected while a dead socket can no longer park a stage indefinitely - which matters more now that AI stages keep several requests in flight |
| **PDF pages as rows** | A great deal of data engineering starts from documents, not tables: filings, annual accounts, invoices, regulatory publications. `src.pdf` gives one row per page - `document_id`, `page_number`, `text`, `has_text_layer`, `width`, `height` and the document's own metadata - from a file or a whole folder, using the text layer the PDF already carries. `document_id` is the same value `src.artifact` puts in `uri`, so a file listing and its pages join without translation. **Wire an artifact relation into it** and the documents are whatever those rows name rather than a configured path, so `src.changed -> Copy Artifact -> PDF pages` is one pipeline: each page carries `document_uri` and the `source_sha256` carried from the row, which is what makes the raw bytes and the parsed rows the same provenance chain. A remote document is fetched to a temporary file because a PDF reader seeks - its cross-reference table is at the end - one document at a time, removed as soon as it has been parsed, so the bound is one document rather than the corpus. With nothing wired in it reads its path exactly as before. There is no OCR, deliberately: rasterising a scanned page needs a native rendering engine and per-language trained data, which would end the self-contained cross-OS build. A scanned page arrives with `has_text_layer` false instead, which is what lets you filter those pages out and route them to whatever OCR you already run |
| **Model cards, not a model store** | Once a pipeline can train a model it needs to answer which model produced this output, and where it lives. `snk.model` records a card - the artifact URI your training script wrote, plus whatever metrics, framework and hashes it reported - to `<folder>/<name>/<version>.json`, with a `latest.json` pointer beside it; `src.model` reads one back as a row, addressed as `name@version` or `name@latest`. The engine never touches the model bytes and never loads a model: the row carries the URI and your Python stage does the rest. What it does add is the part a convention cannot - the card is written only if the whole run succeeded, so a training pipeline that fails afterwards never registers a model and a failed retrain never moves the pointer off the model that still works |
| **Kafka security that is actually applied** | The Kafka form has offered a security protocol, a SASL mechanism, a username and a password since the connector shipped, and the engine read none of them: a node configured for SASL_SSL connected in plaintext, unauthenticated, and said nothing about it. All four are now honoured - TLS reuses the same merged OS-plus-bundled trust store every other connection uses, and PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512 are supported. A mechanism outside that set fails the run naming what is available, rather than quietly downgrading to an unauthenticated connection. *Consumer group* has been removed: the Kafka client Duckle uses implements no consumer groups, so it could never have done anything - use *Resume where the last run stopped* instead, which is the job it looked like it was doing |
| **Kafka that resumes** | Tick *Resume where the last run stopped* on a Kafka source and it remembers the offset it reached, carrying on from there next run. That is what turns a schedule into a stream: without it, *Earliest* re-reads the whole backlog every run and *Latest* skips everything that arrived in between, so repeated runs could never be stitched together. The position is written only when the **whole run succeeded**, so a failure after the read re-delivers those records rather than losing them - at-least-once, deliberately, since the alternative is committing an offset for rows no sink ever wrote. A saved position records the topic and partition it belongs to and is ignored if either changes |
| **Response provenance** | Tick *Add response metadata* on a REST source and every row carries `_http_url` (the exact URL fetched, per page), `_http_status` and `_fetched_at`, so you can tell whether a result changed because the source changed or because the parser did |
| **Reuse a stage's output** | Tick *Reuse this stage's output* on an expensive deterministic stage and it writes its rows once, then reads them back while its SQL, everything above it, and the size/modified time of any local file it reads are unchanged. Off by default and per stage: a cache that guesses when it is still valid serves stale rows silently. `rm -r .duckle/duckle_cache` clears it |
| **Pipeline tests** | `duckle test` runs a pipeline against a fixed input and asserts the rows out of one node. `validate` catches what will not compile; this catches a transform that compiles and computes the wrong thing. The node's WHOLE output is compared, not a sample of it. Comparison is strict: `5` and `"5"` are different, and so are `null`, a missing field and `""` - a case can opt back into text with `"compareAs": "text"`. An expectation can also assert column TYPES with `"schema": {"day": "DATE", "n": "BIGINT"}` - a rendered-value comparison cannot see DATE becoming VARCHAR or BIGINT becoming DECIMAL, since both render identically, and precision is not compared. A type name it does not recognise fails the case rather than quietly meaning VARCHAR. Spell out the precision - `"DECIMAL(18,2)"` - and precision is compared too, because `DECIMAL(18,3)` becoming `DECIMAL(10,2)` rounds and eventually overflows while still reading as the same broad type; a bare `"DECIMAL"` still means the family. SQL without an ORDER BY has no guaranteed order, so `"orderBy": ["id"]` sorts both sides before comparing and `"unordered": true` compares as a bag - neither is on by default, so a case asserting the pipeline own ORDER BY still does. Beyond rows, an expectation takes `rowCount`, `unique`, `notNull`, `tolerance` for float noise, and `sql` for anything else (`SELECT max(amt) < 100 FROM {rows}`) - and with one of those present it need not list rows at all. A source that reads no file (S3 behind a connection, REST, DuckLake) is replaced by a reader for the fixture, so a pipeline can be tested without production credentials. `--json` for CI and agents, and the MCP server exposes the same thing as `run_tests` so a coding agent can run the suite without shell access. Exit 1 on a failed assertion |
| **Run to a node** | `duckle-runner --target <node>` stops at that node and prints its rows; the MCP `run_pipeline` tool takes the same `target`. Nothing downstream runs, so no sink past it writes - the run-from-here the desktop preview uses, for checking one step without executing the rest |
| **Run logs** | Every run writes component-level NDJSON to `<workspace>/logs/<pipeline name>/runtime.log` (start/finish per stage, row counts, durations, `ctl.log` / `ctl.warn` / `ctl.die` messages). Tail it straight into Splunk or Dynatrace. |
| **Schedules** | Cron, fixed-interval, and file-watch triggers, driven by an in-process scheduler. |
| **Context variables** | Per-environment variables; bind any field to one via a Manual / Context dropdown, or reference `${var}` inline. Resolved at run time. |
| **Workspace-relative paths** | Built-in `${workspace}` (alias `${projectroot}`) resolves to the active workspace root, so source / sink paths can be written relative to it and a workspace folder stays portable when copied or moved. No context needed; works in the canvas, schema autodetect, and headless runs. |
| **Run-time path placeholders** | Built-in `${date}`, `${time}`, `${datetime}`, `${timestamp}`, and `${now}` (UTC) stamp the current run time into any path. They resolve fresh on every run (canvas, schedule, headless runner, built bundle), and a sink's parent folder is created automatically, so a path like `${workspace}/exports/${date}/orders.parquet` lands in a new dated folder each day. No context needed. |
| **Cloud credentials** | Saved S3 / GCS / Azure connections become DuckDB SECRETs; cloud reads / writes go through `httpfs`. S3-compatible endpoints (MinIO / R2 / B2) supported via `ENDPOINT` + `URL_STYLE`. |
| **Workspace** | Pipelines, connections, contexts, documents, and routines persist as plain JSON and Markdown files in a folder you choose. |

---

## Clean data before it reaches your AI

Models inherit the quality of their inputs. RAG indexes, embedding stores, and training sets quietly accumulate duplicates, nulls, malformed rows, mixed encodings, and inconsistent schemas. Duckle is built to scrub that data before it lands in a vector store:

- **Deduplicate** with exact Distinct, Uniqueness, and **Fuzzy Deduplicate** (Jaro-Winkler / Levenshtein); use **Record Match** to find near-duplicate pairs with a similarity score
- **Semantic dedupe** with `xf.ai.dedupe` over a precomputed embedding column
- **Profile + describe** every column up front (Column Profile, Describe, Histogram) so issues surface before they reach a model
- **Validate and filter** malformed, empty, or out-of-range records and route failures to a reject port
- **Normalize** types, encodings, casing, and null handling across messy sources (Standardize, Cast, regex / string transforms)
- **Redact PII** (emails, phones, SSNs, credit cards) via `xf.ai.pii` before embedding
- **Chunk + embed** long text via `xf.ai.chunk` -> `xf.ai.embed` for RAG indexing
- **Classify** rows with an LLM (`xf.ai.classify` constrains the model to one of N user-supplied categories)
- **Retrieve with both halves of hybrid search**, locally, no model API required: **Vector Similarity Search** (cosine / L2 / inner product) and **Full-Text Search** (BM25)
- **Land it in your store** - pgvector ships, and **Pinecone**, **Qdrant**, **Weaviate**, **Milvus** all have working sinks that POST batches through each vendor's HTTP API

---

## Engines

Duckle ships a thin shell and installs its engines on first launch.

| Engine | Role | Status |
|---|---|---|
| **DuckDB** | Default execution engine: analytics, file formats, cloud reads, SQL pushdown. Tracking **v1.5.3** (latest stable). A lock-free single-SELECT read (`Engine::query`) powers dives. | Working |
| **Duckie AI Assistant** | Local chat assistant via **llama.cpp** + **Qwen 2.5 Coder 1.5B GGUF**. Downloads ~1.1 GB and needs no network once installed, or point it at your own OpenAI-compatible endpoint and skip the download entirely. Managed as a `llama-server` subprocess exposing an OpenAI-compatible API on `127.0.0.1`. | Installable |
| **SlothDB** | Alternate embedded analytical engine ([SouravRoy-ETL/slothdb](https://github.com/SouravRoy-ETL/slothdb)), installed the same way and selectable per pipeline. | Installable |
| **Native** | In-process Rust streaming / incremental engine. | Planned |

### First-launch extension pre-fetch

When the installer downloads the DuckDB CLI it also pre-fetches the extensions Duckle uses, with per-extension progress, so the first time you touch a Postgres source or an Iceberg table there is no surprise network hop mid-pipeline:

`httpfs` (S3 / GCS / HTTP), `azure` (Azure Blob native), `sqlite`, `postgres`, `mysql`, `excel`, `iceberg`, `delta`, `ducklake`, `vss`, `fts`.

`spatial` is lazy-loaded (~50 MB GDAL bundle) - it installs on first use of a geospatial source/sink to keep the initial download small.

---

## How to use Duckle

A wider tour of the workflow.

| Step | What you do | Where to look |
|---|---|---|
| **1. Sources** | Drag a source, point it at a file / DB / cloud URL / SaaS endpoint. Click **Autodetect schema** to read columns + a sample. | [Sources reference](#sources) |
| **2. Transforms** | Wire transforms to source output ports. Configure in the Properties panel. **Preview** tab shows live rows; **Plan** tab shows generated SQL. | [Transforms reference](#transforms) |
| **3. Data quality** | Drop in a validator (Not-Null, Range, Regex, Uniqueness). Passing rows continue on the main port; failures route to the **reject** port. | [Data quality reference](#data-quality) |
| **4. Sinks** | Finish with a sink (file, DB, cloud, vector DB, message bus, email). Set write mode (overwrite, append, truncate, upsert). | [Sinks reference](#sinks) |
| **5. Run** | Press **Run** to execute on DuckDB. Nodes light up stage by stage; **Output** + **Console** show row counts, timing, errors. Stop button kills mid-run. | [Run feedback](#orchestration-and-workspace) |
| **6. Ask Duckie** | For anything you can describe in English, the AI assistant can sketch a pipeline. Iterate by editing the graph or asking follow-ups. | [Meet Duckie](#meet-duckie---the-local-ai-pipeline-assistant) |
| **7. Reuse** | Save Connections, Context variables, and SQL Routines in the workspace; reference `${context.var}` in any field. Everything persists as plain files. | [Workspace and Git flow](#workspace-and-git-flow) |
| **8. Schedule** | Attach a cron, interval, or file-watch trigger to run a pipeline automatically. | [Schedules and triggers](#schedules-and-triggers) |

---

## Recipes and examples

Ready-to-adapt patterns. Each one is a few nodes you wire on the canvas (or ask Duckie to sketch).

### CSV cleanup

> "Read orders.csv, drop nulls, deduplicate by order_id, write to orders_clean.parquet"

```
src.csv -> qa.not_null -> qa.uniqueness -> snk.parquet
```

Set `qa.not_null` to the columns that must be present; set `qa.uniqueness` to `order_id`. Rejected rows go to a `snk.csv` on the `reject` port for inspection.

### Postgres -> Snowflake nightly load

> "Read all rows from Postgres `events`, upsert into Snowflake table `analytics.events` on `event_id`"

```
src.postgres -> snk.snowflake (mode=upsert, conflict=event_id)
```

Attach a `ctl.schedule` with cron `0 2 * * *` to run nightly at 02:00.

### S3 -> partitioned Parquet

> "Read all .json.gz files in `s3://logs/2026/*/*.json.gz`, parse, write Hive-partitioned by `event_date`"

```
src.s3 (glob, autodetect json.gz)
  -> xf.derive (event_date = CAST(ts AS DATE))
  -> snk.parquet (path=out/, partitionBy=event_date, mode=overwrite_or_ignore)
```

### RAG ingestion

> "Chunk our docs, embed with OpenAI, dedupe near-identicals, store in pgvector"

```
src.s3 (markdown files)
  -> xf.ai.chunk (chunkSize=1500, overlap=150)
  -> xf.ai.pii (redact)
  -> xf.ai.embed (model=text-embedding-3-small, baseUrl=https://api.openai.com)
  -> xf.ai.dedupe (threshold=0.95)
  -> snk.pgvector (table=docs)
```

### Slack channel digest

> "Pull yesterday's Slack messages from #support, classify by sentiment, email a summary"

```
src.slack (channels.history with oldest=yesterday)
  -> xf.ai.classify (categories=positive,negative,neutral)
  -> xf.aggregate (group by sentiment, count)
  -> snk.email (to=oncall@..., subject=Daily Support Digest)
```

### Webhook -> S3 archive

> "Receive 100 webhooks, archive each one as JSON in S3"

```
src.webhook (port=8080, maxRequests=100, timeoutMs=300000)
  -> snk.s3 (path=s3://archive/events/, format=jsonl, partitionBy=event_date)
```

### Git commit-log analytics

> "Build a dashboard of who's been committing what in the last 30 days"

```
src.git (mode=log, maxRows=10000)
  -> xf.filter (date > current_date - INTERVAL '30 days')
  -> xf.aggregate (group by author_email, count)
  -> snk.csv (path=author-stats.csv)
```

More examples live in [`samples/`](samples) - drop the pipeline files into a workspace and open them.

---

## Git integration (GitHub + GitLab)

> Push, pull, branch, and watch CI from inside Duckle. No terminal required.

Click the **Git icon** in the topbar to open the workspace Git panel. Built-in integration with GitHub and GitLab, on the system `git` CLI (no FFI, no embedded git library):

| Feature | What it does |
|---|---|
| **Status snapshot** | Current branch, ahead/behind counts, list of modified / staged / untracked / conflicted files |
| **Stage all + commit** | One-click `git add -A && git commit -m "..."` with your message |
| **Push / Pull** | `git push` and `git pull --ff-only` against `origin`. The button stays disabled when there's nothing to push |
| **Branch list, switch, create** | Lists local branches; click to switch; create new branches inline |
| **Remote URL config** | Add or change `origin` URL from inside the panel - auto-detects GitHub vs GitLab from the host |
| **PAT-prompt fallback** | First tries `git push` using your system credential helper (GitHub CLI, osxkeychain, manager-core). On a 401, prompts for a Personal Access Token, saves it AES-encrypted in `<workspace>/.duckle/secrets/git.json` (auto-gitignored), retries with the token injected into the HTTPS URL |
| **CI build badge in topbar** | Polls GitHub Actions or GitLab CI every 30 s for the latest pipeline on your current branch. Shows green / red / yellow / gray. Click to open the build in your browser |

**Workflow.** Workspaces are plain folders (see [Workspace and Git flow](#workspace-and-git-flow)) - any standard Git workflow works:

```
Create / clone -> open in Duckle -> edit pipelines -> commit + push -> 
PR / MR -> CI runs your pipeline tests -> merge -> pull
```

You can do the entire push / pull / merge loop without leaving Duckle. Heavy operations (interactive rebase, conflict resolution, log archaeology) still live in your terminal or external Git tool - the panel is designed for the everyday flow, not as a full Git replacement.

**Provider detection.** The remote URL host determines which CI API the badge polls:

| Provider | CI source | API |
|---|---|---|
| `github.com` | GitHub Actions | `GET /repos/{owner}/{repo}/actions/runs` |
| `gitlab.com` or self-hosted GitLab | GitLab CI | `GET /api/v4/projects/{id}/pipelines` |
| Other / bitbucket | (no CI badge for now) | - |

The badge uses the same PAT you saved for pushes - no separate auth step.

---

## Workspace and Git flow

A workspace is a folder you pick on first launch. Everything you build lives there as plain text:

```
my-workspace/
  pipelines/
    orders_etl.pipeline.json     # the node graph
    nightly_load.pipeline.json
  connections/
    prod-postgres.connection.json # saved DB credentials (encrypted)
    snowflake-analytics.connection.json
  contexts/
    dev.context.json              # variables for dev environment
    prod.context.json
  routines/
    cleanse-addresses.sql         # reusable SQL snippets
  documents/
    runbook.md                    # plain-Markdown docs
  schedules.json                  # all scheduled runs in this workspace
  run-history/
    orders_etl/                   # one folder per pipeline
      2026-05-25T14-30-00.json    # one file per run
```

**Git-friendly by design.** Every file is human-readable JSON or Markdown. Standard workflows work:

```bash
git init my-workspace && cd my-workspace
git add . && git commit -m "Initial pipelines"

# Pull a teammate's update
git pull --rebase

# Push your changes
git push

# Branch for a risky migration
git checkout -b feature/upsert-mode
# ...edit pipelines in Duckle...
git diff       # readable JSON diffs
git push -u origin feature/upsert-mode
# open PR / MR
```

**Sensitive values** in connections get encrypted with a workspace-local key (`workspace/.duckle/keys/`). Don't commit that file - add `**/.duckle/keys/` to `.gitignore`. The connection JSON files themselves only hold the ciphertext, which is safe.

---

## MCP server (connect Claude or any LLM to Duckle)

<p align="center"><img src="docs/assets/mcp-claude-banner.svg" alt="Connect Duckle to Claude via MCP" width="92%"/></p>

Duckle ships its own [Model Context Protocol](https://modelcontextprotocol.io)
server, so Claude (or any MCP client - Claude Desktop, Claude Code, Cursor, or
any other LLM agent) can drive Duckle directly: browse the full component catalog
and per-component property schemas, **generate a pipeline straight into a working
directory you choose**, validate it (compile without running), run it headlessly,
read existing pipelines and their run logs, build a standalone artifact, and
manage saved connections.

### Connect with nothing installed

If you have [uv](https://docs.astral.sh/uv/), one line connects any MCP client. Nothing is installed, no engine to configure: uv fetches the package and the DuckDB engine into a throwaway environment and the server finds it there.

```sh
claude mcp add duckle -- uvx duckle mcp
```

For Claude Desktop, Cursor, or any other client, the same thing as config:

```json
{ "mcpServers": { "duckle": { "command": "uvx", "args": ["duckle", "mcp"] } } }
```

`uvx duckle mcp` works because the package and the command are both named `duckle`, so there is no `--from` to remember. If you would rather install it, `pip install duckle` puts `duckle` on PATH and the same `duckle mcp` command applies.

Then ask the agent something like *"use duckle to list the available components"*. It can discover a real connector rather than guess one, compile-check a pipeline with `validate_pipeline` **before anything executes**, run it, and hand back column-level lineage. What it produces is the same JSON the canvas opens, so you can see what it built.

### Connect in one click

The MCP server is also **bundled inside the app** - there is nothing extra to install.
In the designer, click **Connect to Claude** in the top bar to open the connector
popup, then pick your client:

- **Connect to Claude Code** - registers the `duckle` server for you (runs
  `claude mcp add` under the hood).
- **Add to Claude Desktop** / **Add to Cursor** - writes the `duckle` entry into
  that client's config, with the resolved engine paths filled in (both the
  Microsoft Store / MSIX and standalone Claude Desktop layouts are handled).
- Or copy the command / config for any other MCP client.

Restart the AI client, then try *"Use duckle to list the available components"*
to confirm the connection.

### Manual / headless

For a build-from-source or server setup, point any client at the `duckle-mcp`
binary directly. It speaks JSON-RPC over stdio and reuses the DuckDB engine
in-process (no GUI, no Node runtime).

```bash
cargo build -p duckle-mcp --release      # target/release/duckle-mcp
claude mcp add duckle -- /path/to/duckle-mcp
```

For Claude Desktop and other clients, add it to `mcpServers`:

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

Tools: `list_components`, `get_component_schema`, `create_pipeline`,
`validate_pipeline`, `run_pipeline`, `list_pipelines`, `read_pipeline`,
`read_run_logs`, `build_pipeline`, `list_connections`, `create_connection`, `backfill_list`, `backfill_set`, `backfill_clear`.
`run_pipeline` / `build_pipeline` need a DuckDB binary (`DUCKLE_DUCKDB_BIN`);
`build_pipeline` also needs `duckle-runner` (`DUCKLE_RUNNER_BIN`). Full guide:
[docs/current/mcp.md](docs/current/mcp.md).

---

## Connection management

Saved connections become DuckDB secrets at runtime so credentials never leak into the pipeline JSON.

| Type | Stored fields | Used by |
|---|---|---|
| **PostgreSQL / MySQL / etc.** | host, port, user, password, database, ssl mode | `src.postgres`, `snk.postgres`, ... |
| **Snowflake** | account, user, role, warehouse, PAT or JWT private key | `src.snowflake`, `snk.snowflake` |
| **S3 / GCS / Azure** | access key, secret, region (or service-account JSON) | All cloud sources/sinks via `httpfs` |
| **MotherDuck / Databricks / BigQuery** | token, workspace URL | Respective sources/sinks |
| **Generic REST / SaaS** | base URL, headers, auth scheme (Bearer / Basic) and token | All REST aliases |

Connections live in `workspace/connections/` as JSON. The token/password field is encrypted with the workspace key; the rest is plain text.

To use a connection in a pipeline, the Properties panel of any compatible source/sink shows a **Connection** dropdown - pick one and the fields auto-fill. The list is filtered to connections of a matching kind, so a REST connection is not offered on a JDBC node.

A **REST** connection is the exception to auto-fill, because it exists to be shared by many nodes that each send a different request: put the vendor's headers and token on the connection once, and rotating a key is a single edit. Headers are merged per key at run time, and the node wins on a key it sets itself; the node's own `url` and request body are never overwritten. A node with no URL of its own inherits the connection's.

The **Copy SQL** / **Export SQL** output is display-only and never executed. Secret values (passwords, tokens, keys, connection strings) are replaced with named placeholders such as `${DUCKLE_PASSWORD}`, so the exported script stays valid and is safe to share - substitute the real value at run time. To emit the real credentials instead (so the script runs unchanged), set the environment variable `DUCKLE_EXPORT_INCLUDE_SECRETS=1`; the output then contains live secrets and should be handled accordingly.

---

## Context variables

Bind any field to a context variable that resolves at run time. Useful for `dev` vs `prod`, per-environment paths, secrets injected from CI, etc.

In a context file (`workspace/contexts/prod.context.json`):

```json
{
  "name": "prod",
  "vars": {
    "DB_HOST": "db.internal.acme.com",
    "S3_BUCKET": "acme-prod-data",
    "BATCH_SIZE": "10000"
  }
}
```

In the Properties panel of any node, switch a field from **Manual** to **Context** and pick `DB_HOST`. Or inline-reference one with `${DB_HOST}` in a string field.

Pick the active context from the topbar's **Context** dropdown. Switch contexts and re-run without editing the pipeline.

---

## Build from source

**Prerequisites**

- [Rust](https://rustup.rs/) (stable)
- [Node.js](https://nodejs.org/) 18+ and npm
- [`cargo-tauri`](https://tauri.app/) CLI: `cargo install tauri-cli --version "^2"`
- Platform webview dependencies per the [Tauri prerequisites](https://tauri.app/start/prerequisites/). WebView2 is preinstalled on Windows 10 and 11.

**Clone and install**

```bash
git clone https://github.com/slothflowlabs/duckle
cd duckle
npm --prefix frontend install
```

**Run in development** (hot-reloading frontend plus the native shell):

```bash
cargo tauri dev
```

**Build a release binary:**

```bash
# The --features custom-protocol flag is required: without it, tauri-codegen
# embeds the dev URL instead of the bundled frontend.
cargo build --release --manifest-path apps/desktop/Cargo.toml --features custom-protocol
```

Outputs land in `target/release/duckle` (or `duckle.exe`). The engine is not statically linked: DuckDB downloads at first launch, which is why the build is fast and the binary is tiny.

**Run the tests:**

```bash
cargo test                                                          # workspace unit + plan tests
DUCKLE_DUCKDB_BIN=/path/to/duckdb cargo test -p duckle-duckdb-engine # full integration suite
```

---

## Architecture

```
duckle/
  apps/desktop/         Tauri 2 shell: Tauri commands, engine installer, llama runtime, window
  frontend/             React 19 + Vite + TypeScript: the designer UI + chat panel
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
- **Duckie** is a `llama-server` subprocess on `127.0.0.1` exposing an OpenAI-compatible chat-completions API. The chat panel streams from it via SSE. The model is sandboxed: no fs, no net, no tools - it can only emit text.
- **Everything persists** to the workspace folder you choose, as plain JSON and Markdown files.

---

## Configuration

A few knobs you can set without touching code.

| Setting | Where | Effect |
|---|---|---|
| **Theme** | Topbar sun/moon toggle | Light / dark, persisted to `localStorage` |
| **Workspace** | Topbar workspace pill -> Switch | Change the folder Duckle reads/writes to |
| **Active engine** | Topbar engine selector | DuckDB (default) or SlothDB - per-pipeline |
| **Active context** | Topbar context dropdown | Switches which context variables resolve at run time |
| **AI Assistant baseURL** | `xf.ai.llm` / `xf.ai.embed` / `xf.ai.classify` props | Point at any OpenAI-compatible endpoint (default: Duckie's local llama-server) |
| **Per-stage retry** | Properties panel -> Advanced tab | Total attempts + linear-scaled backoff per stage |
| **Per-stage memory cap** | Properties panel -> Advanced tab | `PRAGMA memory_limit` applied just to that stage |
| **Per-stage materialize** | Properties panel -> Basic tab | `auto`, `view` (lazy), `memory` (read once, table in RAM), or `disk` (read once, streamed via a temp Parquet file for huge intermediates) |
| **DuckDB extensions** | Pre-fetched at install; lazy-loaded for `spatial` | See [First-launch extension pre-fetch](#first-launch-extension-pre-fetch) |
| **Env var `RUST_LOG`** | Before launching the binary | `RUST_LOG=debug duckle.exe` to see verbose engine logs |
| **Env var `DUCKLE_DUCKDB_BIN`** | Before running engine tests | Points the integration test suite at a DuckDB CLI |
| **Env var `DUCKLE_CA_CERT`** | Before launching the binary | Path to a PEM bundle of extra CA certificates to trust (corporate proxy / private CA), added on top of the OS trust store and bundled roots |
| **Env var `DUCKLE_HTTPS_PROXY`** (or standard `HTTPS_PROXY` / `HTTP_PROXY` / `ALL_PROXY`) | Before launching the binary | Routes REST / cloud-API connectors and the in-app updater through an HTTP proxy, e.g. `http://user:pass@proxy:8080`. Use the standard vars to also cover engine / model downloads |

---

## Performance tips

A few patterns that consistently produce sub-second runs at small / medium data scale, and tractable runs at warehouse scale.

| Tip | Why |
|---|---|
| **Use Parquet, not CSV, for intermediate steps** | Columnar + compressed; DuckDB reads only the columns the next stage needs. CSV is fine for source / sink at the edges. |
| **Push filters as early as possible** | `xf.filter` early in the graph compiles to a `WHERE` that runs at scan time, not a post-scan filter. |
| **Use the `vss` + `fts` indexes** | Vector + full-text search hit DuckDB extensions directly. Faster than the alternative of pulling data out and indexing in Python. |
| **Avoid per-row API calls when batch APIs exist** | `xf.ai.embed` batches up to 100 inputs per request; `snk.rest` defaults to one batched request. Per-row patterns (`xf.ai.llm`, `snk.webhook`) are slower by design - use them when you actually need per-row behavior. |
| **Cap heavy aggregates with the per-stage memory limit** | Properties panel -> Advanced -> Memory limit (MB) prevents one big GROUP BY from blowing through all of RAM. |
| **Use `ctl.checkpoint` for long-running pipelines** | A checkpoint stage writes a Parquet snapshot to a path you choose, so a future run can resume from there with `src.parquet`. |
| **Disable `xf.debug.log` in prod** | Logging rows is per-row I/O; fine for dev, costly at scale. |
| **Sort once at the end, not in the middle** | `xf.sort` is a global sort; doing it once before the sink avoids re-sorting downstream. |
| **Put an `xf.dbt` node behind its upstream, not first** | When a dbt node has upstream stages, Duckle warms dbt's project parse in the background while those stages run, so `dbt run` reuses a warm cache instead of paying a cold parse. Set `DUCKLE_DBT_PREWARM=0` to disable. |

---

## FAQ

<details>
<summary><b>Is Duckle free? What's the license?</b></summary>

Yes, free + open source. Dual-licensed **MIT OR Apache-2.0**. You can use it commercially, fork it, sell what you build with it. No usage limits, no telemetry.

</details>

<details>
<summary><b>Is Duckle an open-source alternative to Fivetran or Airbyte?</b></summary>

It covers similar ground - moving data across 190 sources and destinations - but locally, with nothing to host and no per-row, per-connector, or per-seat billing. Pipelines are built visually or from plain English and compile to readable DuckDB SQL that runs wherever you deploy it: a laptop, a server, CI or a container. The trade-off is scope: Duckle does not split one query across a cluster, so for warehouse-scale replication you push the work down into the source system or point the output at the system that scales.

</details>

<details>
<summary><b>Can I run ETL pipelines without the cloud or a data warehouse?</b></summary>

Yes. Duckle executes on the embedded DuckDB engine, so there is no vendor warehouse to buy, no vendor platform to sign up to, and no account. You run it where you choose: a server or VM you own, a container in your own AWS, Azure or GCP account, or a workstation. It needs no outbound network of its own, which suits air-gapped, on-premise and compliance-sensitive work. Pipelines still read from and write to cloud systems whenever you point them at one.

</details>

<details>
<summary><b>How is Duckle different from Airbyte, dbt, or Talend?</b></summary>

Airbyte focuses on hosted extract-and-load connectors; dbt focuses on SQL transformation; Talend is a heavyweight GUI suite (its free Open Studio edition was discontinued in early 2026). Duckle is a single open engine that does extract, transform, and load together - write it in Python, wire it from connectors, or draw it on a canvas - compiles to DuckDB SQL, and can also run dbt on DuckDB inside the same tool. One format, one engine, running on your own infrastructure rather than a vendor's, with no per-row billing.

</details>

<details>
<summary><b>Does Duckle send my data anywhere?</b></summary>

No. Duckle makes no outbound calls of its own from wherever you run it, laptop or server. The engines (DuckDB, llama.cpp) are downloaded from official upstream releases on first launch and then run in place. The only network calls Duckle makes on your behalf are the ones your pipelines explicitly do (e.g. a `src.s3` reading from your S3 bucket, or `xf.ai.embed` if you configure it to hit OpenAI).

Duckie needs no network once its model is downloaded - and if you would rather it did not run in-process at all, point it at your own OpenAI-compatible endpoint.

</details>

<details>
<summary><b>How big are pipelines this works well on?</b></summary>

Bigger than people assume, because the ceiling is the instance you provision rather than the laptop you develop on. The engine is parallel and uses every core available, so the same pipeline that you debug against a sample on a laptop runs against the full set on a large server without changing. For reference, 96M rows come out of live Postgres to Parquet in 39.9s.

Past whatever instance you are willing to pay for, you have two routes that do not involve rewriting anything: turn on pushdown so the query executes inside the source database, or point the output at a warehouse or lakehouse that scales horizontally. What Duckle will not do is spread a single query across a cluster.

</details>

<details>
<summary><b>Do I need DuckDB installed first?</b></summary>

No - Duckle downloads it for you on first launch. The download is ~30 MB and includes the most-used extensions (httpfs, postgres, mysql, iceberg, delta, vss, fts, etc.) so the first time you touch a Postgres source there's no mid-pipeline network pause.

</details>

<details>
<summary><b>How big is the binary, exactly?</b></summary>

73 to 110 MB, depending on platform. As of v0.7.0: macOS 73 (x64) to 88 (arm64), Linux 74 (arm64) to 100 (x64), Windows 98 (arm64) to 110 (x64). It embeds the headless runner and the MCP server, and the headless runner on its own is 27 MB. The engines aren't statically linked - DuckDB (~50 MB with extensions) and the Duckie LLM (~1.1 GB for the Qwen GGUF) both download on first launch with a guided installer into your app-data folder, so they update independently of the app.

</details>

<details>
<summary><b>Can I use OpenAI / Cohere / Voyage instead of the local Duckie?</b></summary>

Yes. The AI transforms (`xf.ai.embed`, `xf.ai.llm`, `xf.ai.classify`) accept a `baseUrl` prop. Point it at any OpenAI-compatible `/v1/...` endpoint and an `apiKey` and Duckle uses that instead. The local Duckie chat panel is hardwired to localhost; the pipeline AI transforms are configurable.

</details>

<details>
<summary><b>Where does my pipeline data live?</b></summary>

In the workspace folder you pick on first launch (see [Workspace and Git flow](#workspace-and-git-flow)). Pipelines are plain JSON files you can commit to Git, diff, branch, and review.

</details>

<details>
<summary><b>Can multiple people collaborate on the same workspace?</b></summary>

Via Git, yes - check the workspace into a repo and use standard branch/PR flows, and deploy the result to a shared server where the console has roles and an audit log. What there is not is a real-time multiplayer canvas: two people editing the same pipeline at the same moment is a merge, not a live session.

</details>

<details>
<summary><b>Can I run pipelines headlessly / from CI?</b></summary>

Yes. **Build Pipeline** (right-click a pipeline) produces a single self-contained executable that runs anywhere with nothing installed - drop it on a server or CI runner and execute it, or schedule it with cron / systemd / Task Scheduler. The embedded `duckle-runner` can also run a workspace pipeline JSON directly (`duckle-runner --pipeline pipeline.json`). See [Server deployment](#server-deployment-build-pipeline). You can also import the engine crate (`duckle-duckdb-engine`) into your own Rust binary.

</details>

<details>
<summary><b>Is the Duckie AI assistant any good?</b></summary>

For 90% of common pipelines (read source -> simple transforms -> sink), yes - the Qwen 2.5 Coder model is tuned for structured-JSON generation. For long, complex pipelines you'll likely want to iterate: describe the first half, click insert, then ask for the next half. You can also swap the model: point `xf.ai.llm`'s `baseUrl` at GPT-4 or Claude for more capable pipeline drafting.

</details>

<details>
<summary><b>Does the Duckie panel need internet after install?</b></summary>

No. Once `llama-server` and the Qwen GGUF are downloaded into your app-data directory, Duckie needs no network at all. Nor does it have to run in-process: point it at your own OpenAI-compatible endpoint and it uses that instead. Tested by killing wifi and asking it for a pipeline - works fine.

</details>

<details>
<summary><b>Why DuckDB and not Polars / Apache Spark / X?</b></summary>

DuckDB's SQL surface is wide enough to express most ETL work, it's vectorized and fast on a laptop, it has first-class Iceberg/Delta/Parquet readers, and its extension model lets us add vector + full-text + Postgres ATTACH without code changes. Polars is great but doesn't ship the cloud/format/extension breadth we need; Spark is a great cluster but overkill for the local-first niche we're in.

</details>

<details>
<summary><b>How do I contribute a new connector?</b></summary>

See the [Contributing](#contributing) section and `crates/duckdb-engine/src/plan.rs` (planner branch) + `crates/duckdb-engine/src/lib.rs` (executor). The shortest path: copy an existing connector with similar shape (e.g. `src.rabbit` for a streaming source, `src.dynamodb` for an HTTP+auth API), adapt, add a test, flip the palette tile.

</details>

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| **Window opens but content shows "localhost refused to connect"** | Release binary built without `--features custom-protocol` (the v0.0.7 bug) | Rebuild with `cargo build --release --features custom-protocol` per [Build from source](#build-from-source). The release workflow already passes this flag. |
| **"DuckDB CLI not found"** on Run | First-launch installer was skipped or interrupted | Open the engine setup modal from the toolbar; click Install on DuckDB |
| **"Couldn't download Duckie AI Assistant (HTTP 404)"** | Pinned llama.cpp build temporarily unavailable from upstream | Bump `LLAMACPP_BUILD` in `apps/desktop/src/engine_manager.rs` to a recent stable, rebuild |
| **Linux: app won't launch, missing libwebkit** | WebKitGTK 4.1 isn't installed | `sudo apt install libwebkit2gtk-4.1-0` (Debian/Ubuntu) or your distro's equivalent |
| **macOS: "App can't be opened because Apple cannot check it"** | Gatekeeper, unsigned binary | Right-click the binary -> Open -> Open Anyway |
| **Pipeline runs but a connector errors with "extension not loaded"** | Lazy-loaded extension (e.g. `spatial`) downloaded mid-run and failed | Run `duckdb :memory: -c "INSTALL spatial; LOAD spatial;"` from a terminal to pre-install; relaunch Duckle |
| **Chat panel says "AI engine not registered"** | Old version of Duckle before AI shipped (pre-v0.0.10) | Update to latest release |
| **Duckie generates a pipeline but Insert doesn't put anything on the canvas** | Active pipeline tab has been closed; nothing to insert into | Open a pipeline (or create a new one) before clicking Insert |
| **MotherDuck / Snowflake auth fails** | Token expired, or PAT lacks the role you're trying to use | Regenerate in the vendor UI; paste into the Connection in Duckle |
| **Postgres `ATTACH` says "could not connect"** | Local SSL mode mismatch | Connection -> Advanced -> set SSL mode to `disable` for localhost / `require` for production |
| **AI tests skip with no failure** | `DUCKLE_DUCKDB_BIN` isn't set | `export DUCKLE_DUCKDB_BIN=/path/to/duckdb` before `cargo test` |
| **TLS "UnknownIssuer" / "invalid peer certificate" behind a corporate proxy** | A TLS-inspecting proxy (Zscaler, Netskope, ...) re-signs traffic with its own CA | Duckle trusts your OS certificate store on top of its bundled roots, so the proxy CA in the Windows / macOS / Linux store is honoured automatically. If the CA isn't in the store, point `DUCKLE_CA_CERT` at a PEM file containing it. Note: DuckDB's own extension fetch (`extensions.duckdb.org`) and cloud reads (S3 / GCS / Azure) run inside the DuckDB engine with its own TLS, so also allow / exempt `extensions.duckdb.org` from inspection. |
| **REST / cloud calls fail with "Connection Failed" / timeout (os error 10060)** behind a proxy | The network requires an HTTP proxy to reach the internet, and Duckle is connecting directly | Set `HTTPS_PROXY` (and `HTTP_PROXY`) to your proxy URL, e.g. `http://user:pass@proxy:8080`, before launching Duckle - REST / cloud connectors and the updater now route through it. Use `DUCKLE_HTTPS_PROXY` if you want a Duckle-only proxy without changing global env. |

If you see something not listed, please [open an issue](https://github.com/slothflowlabs/duckle/issues) with steps to reproduce + the relevant log line.

---

## CI / CD

Duckle's CI pipeline runs on **both GitHub and GitLab** - the project mirrors to both. Push / pull-request / merge-request / tag events all trigger builds.

| Trigger | GitHub Actions | GitLab CI |
|---|---|---|
| **Push to main or feature branch** | `.github/workflows/ci.yml` | `.gitlab-ci.yml` (`test` + `desktop-build` stages) |
| **Pull request / merge request** | `.github/workflows/ci.yml` | `.gitlab-ci.yml` (same stages, `rules:` gate on MR events) |
| **Tag `v*`** | `.github/workflows/release.yml` | `.gitlab-ci.yml` (`release` stage; uploads binaries to GitLab Releases) |

What each pipeline does:

1. **Frontend** - `npm ci` + `npm run build` (type-check + bundle)
2. **Rust test matrix** - `cargo test --workspace` on Linux + macOS + Windows
3. **Live-service integration tests** - PostgreSQL + MySQL + MinIO services spun up via Docker, real connector code runs against them
4. **Desktop release-build smoke check** - `cargo build --release --features custom-protocol` then grep the binary for the embedded frontend JS chunk (catches the v0.0.7-class "binary loads devUrl" bug at PR time)
5. **Format + clippy** - informational (does not block merge)
6. **On tag**: build the Duckle binary on all three OSes, upload as release assets

See [`.github/workflows/`](.github/workflows/) and [`.gitlab-ci.yml`](.gitlab-ci.yml) for the exact steps. The two pipelines are kept feature-equivalent so contributors can fork to either platform.

### Releasing a new version

Nothing regenerates this README, the hero / flow SVGs, or the download
links automatically - they are hand-maintained, so they drift unless each
release updates them. Treat the README as a release artifact: walk this
checklist every time before tagging.

```bash
# 0. Update the README in the SAME commit as the version bump:
#    - bump every vX.Y.Z reference (the Download / Install link, badges)
#    - refresh capability tables for any new sources/transforms/sinks
#    - add/replace screenshots in docs/assets for shipped features
#    - re-check the hero/flow SVG wording if positioning changed
# 1. Bump version in apps/desktop/tauri.conf.json
# 2. Commit (README + version together)
git commit -am "Release: bump to vX.Y.Z"
# 3. Tag + push
git tag vX.Y.Z
git push origin main vX.Y.Z
# Both GitHub Actions and GitLab CI pick up the tag and build the
# release artifacts automatically. Once green, the draft release on
# GitHub gets the binaries uploaded; un-draft + mark Latest with:
gh release edit vX.Y.Z --draft=false --latest
```

---

## What's new in v0.7.1

Ten days and 220 commits. A legacy Talend estate that imports and runs, a REST
fan-out that survives millions of parent rows and resumes where it died, a
ceiling on the AI bill, and boundaries that hold where the act happens rather
than where it was planned.

- **A Talend estate imports and runs, not just parses.** 57 commits of it, and most are a specific reading that turned a working step into a broken one. Mapper expressions translate (the character helpers, dates, counters, conditions, the shipped routines, arithmetic written with signs, a comma inside a literal that is not an argument separator), mapper semantics survive (outputs kept apart, the condition deciding which rows reach an output, a lookup joined rather than dropped and travelling with the loop it feeds, declared types including exact decimals), and job structure survives (a loop body becomes a pipeline the loop can name, a reusable body is spliced into its caller keeping its boundary ports, ordering links order the run without becoming data edges and never close a loop). Context references resolve from the job's own context, and a Java body that only sets context values is carried over to nodes. What cannot be translated **refuses to compile** rather than silently doing nothing, and the report says which Java bodies carry no rules.
- **A REST fan-out that survives two million parents.** One request per row of an upstream table, with the response stamped onto the rows it produced. Parent rows are streamed rather than held in memory. A fan-out that died at row 900,001 resumes: each successful parent is recorded as it completes and replayed on rerun without reissuing the request. The incremental cursor reaches the request, and does **not** advance past a parent that failed under `skip` or `reject`, because advancing past rows that were never fetched loses them for good. The original response is kept, named by its own content, and can be archived to object storage.
- **A ceiling on the AI bill, and a run that admits it stopped.** Request, token and cost budgets, enforced with a compare-and-swap so the ceiling is exact under concurrency, and a stop is not counted as a purchase. Hitting a budget marks the run **incomplete** rather than reporting a truncated dataset as a clean success, and that marker reaches the run history rather than only the CLI. Structured output asks for a shape and checks the reply is that shape, refusing a field that would collide with, or overwrite, an upstream column.
- **A stage whose inputs did not change does not run again.** An opt-in reuse cache keyed on the stage config, its input fingerprint and the engine version, so an engine upgrade invalidates it. Inspect, drop and distrust it from the CLI.
- **Python runs in the environment you declared.** The workspace's own virtual environment is used, and a run whose `.venv` is not what `uv.lock` describes is refused rather than run against the wrong packages. A script can be handed the whole table through Parquet, or streamed.
- **Read the schema the feed already published.** Point `src.xml` at an XSD and the columns come from it, with the exact schema bytes recorded in the signed run manifest. XML, HTML and PDF can all read a corpus an upstream relation names rather than one configured path. HTML reads rows by CSS selector and follows the pagination a server rendered; a walk cut short by a failed page reports as incomplete instead of as a clean run. PDF gives one row per page. Archives unpack into artifacts.
- **Continuous runs.** `follow` for pipelines that track their position, `listen` and `src.spool` so a push source stops losing what arrives, `src.changed` to poll a remote source (including S3) without downloading it, and `xf.tumble` for event-time windows that survive between runs. Kafka resumes where the last successful run stopped, decodes Confluent-framed Avro, and applies the security settings the form has always offered.
- **`duckle test`: assert what a pipeline produces from a fixed input.** Row count, uniqueness, not-null, a SQL predicate, numeric tolerance, column **types** rather than only rendered values, DECIMAL precision where the expectation spells it out, and deterministic ordering for a case that never promised one. An agent can run the suite over MCP without a shell.
- **Boundaries that hold where the act happens.** The policy is enforced at the point of the act, not only at plan time, so a URL built from a row or a redirect hop meets the same refusal. DuckDB is now inside that boundary: under `mode: enforce` with a domain allowlist, a run starts with DuckDB's remote filesystems disabled and community extensions refused, both of which DuckDB will not let a later statement undo. `${VAULT:NAME}` fetches a credential at run time and now resolves on **every** way of running a pipeline, including the MCP server, which had been handing the connector the literal placeholder. SFTP remembers host keys, so an unpinned connection notices a change.
- **Neo4j, Turso/libSQL and IBM DB2**, GeoParquet from the Geospatial sink, a Snowflake `writeMode` that replaces rather than appends, and DuckLake maintenance and multi-table snapshots through the same pipelines that fill the lake.
- **Server and backfill.** Accept a run, answer for it later, and cancel it. Headless backfill across the console API, MCP and the web editor, without a backfill set destroying other state. A resource budget so one job cannot take the machine down.
- **Silent-data fixes.** Describing a node in the editor used to run that node's SQL, which for a sink meant executing its `COPY ... TO` against an empty stub and truncating the real output file on a click. A JSON column that appears late is no longer lost, Flatten actually flattens, a headerless CSV takes its names from its declared schema, GCS carries the bucket's region into the secret, a model is registered only when the run succeeded, and XML entities stopped being dropped.
- **21 dependency commits**, including axum 0.8, aes-gcm 0.11, ed25519-dalek 3, quick-xml 0.42, odbc-api 29, the arrow family onto one major, TypeScript 7 and Vite 8, with advisories reaching the shipped binary patched. The encrypted-secret format was **proved** to survive the crypto majors rather than assumed to.

**Upgrade note.** If your policy sets `network.allowedDomains` with `mode: enforce`, DuckDB itself is now off the network, so a pipeline that relied on DuckDB reading `https://` or `s3://` directly fails closed instead of quietly bypassing the allowlist. Route the read through a Duckle connector, or set `network.allowDuckdbExternalIo: true` in the server policy. Local file access is untouched.

---

## What's new in v0.7.0

A server somebody can set up in a browser, an ordered plan of pipelines, a
catalog of the whole workspace, and a run that stopped reading the source
three times to answer one question.

- **Set a server up without a terminal.** The console used to assume whoever deployed it would also configure it over SSH. It now answers a first visit with a setup page: pick where the server runs, claim it, and the administrator token is shown once. Claiming is the whole setup. A liveness probe at `/healthz` means an orchestrator can tell a starting server from a wedged one, and a schedule that stops working now says so instead of failing quietly.
- **Sign-in, roles and an audit log.** Accounts and machine credentials live in SQLite rather than a file two processes could erase for each other. People are managed from a **People** tab instead of a deployment task, machines get their own API keys, and sessions survive a restart, expire, and travel marked. Roles are enforced in one place per surface, so a route cannot be reached by forgetting to check it at the call site. The audit log records who did what, including refusals, and can be read back from the console.
- **Plans: several pipelines, in an order somebody chose.** A plan is a list of pipelines that run in sequence. Plans are authored in the desktop app and in the console, share one `plans.json`, and can be scheduled like a single pipeline. A plan whose pipelines failed no longer reports that it worked.
- **A Data Catalog across pipelines, not inside one.** The workspace graph now spans pipelines: which asset feeds which, what is orphaned, who owns it, and how fresh it is, because every run records what it touched. Columns, descriptions, tags and a glossary are editable in a **Data Catalog** screen, exposed to the console and to agents through MCP, and a saved graph can tell you when the pipelines have moved on.
- **Queued work, and a worker that claims it.** A ForEach can dispatch its items as a batch rather than running them here. A worker claims queued items, the console shows what is queued and lets you retry what failed, and the queue says whether items can safely run at once. Each sub-pipeline runs under its own name and keeps its own watermark.
- **Runs stopped scanning the source three times.** Every node's row count was a separate `SELECT COUNT(*)`, and since nodes are views, each one re-ran the whole chain. A source to filter to sink pipeline read a 96M-row Postgres table three times to do one pass of work. Each relation is now counted once, and a sink takes its count from the Parquet footer of the file it just wrote, which is a metadata read: 0.06s against 16.7s for the equivalent count over the source. Measured on that pipeline, baseline against this release, interleaved on one machine: 56.3s to 18.8s, and 288,159,946 tuples scanned down to 96,011,803. That puts it level with a hand-written DuckDB `COPY` doing the same work, at 1.02x. A remote XML stream over SFTP was reading 8 KiB per round trip and now reads 256 KiB: 75 MB and 700,000 rows went from 17.0s to 10.0s.
- **Security fixes, two of them serious.** The streaming run route accepted work without authentication, and the in-app updater pointed at a GitHub organisation nobody had registered, so whoever claimed the name could have served the next update. Both are closed. Beyond those: connection secrets are bound to the field and connection they belong to, so a ciphertext cannot be moved between fields; bundle keys are derived with Argon2id and a per-bundle salt instead of an unsalted hash; downloaded engines and models run through a checksum gate that fails closed on a mismatch, though the digests themselves are not yet pinned, so today it warns and proceeds (see `UNPINNED` in `engine_manager.rs`, and issue #288); run parameters can no longer redefine builtins or inject shell syntax; sidecars stage in a private directory instead of shared temp; decrypted connection secrets stay out of browser storage; a cached git token is re-encrypted and never written world-readable; and a deploy refuses to send a pipeline carrying a credential in plain text.
- **Saved connections are a reference, not a copy.** Picking a saved connection used to copy its values onto the node, so the credential was duplicated into the pipeline file and a later edit to the connection did not reach it. The node now stores only the reference and resolves it at run time. The editor shows what the connection will supply rather than the manifest's default, so a node pointing at a connection on a non-standard port stops displaying the standard one, and a secret says only that it is covered.
- **Home is a launcher, and the tour goes first.** The app opens on three tiles rather than dropping you into a canvas, modules are one level in, and a first run walks every capability once instead of showing the tour and Home at the same time. Settings can ask again.
- **Deploying is documented, including the uncomfortable parts.** A deployment guide for AWS, Azure and Google Cloud, the client and server architecture, promoting from CI, driving Duckle from another orchestrator, and a walkthrough of the whole server flow. The docs now say plainly that a failed run still answers 200, before somebody trusts it.

---

## What's new in v0.6.1

Talend jobs import straight into the canvas, and credentials are masked more
carefully in exported SQL.

- **Import a Talend job from the editor.** A **Talend** button sits in the project sidebar next to New Pipeline and New Folder, and the same action is in the editor's **⋯** menu as **Import Talend job...**. Either one picks a `.item` job, translates it, and opens it as a new pipeline tab, laid out on the canvas at the coordinates the job was drawn with. Measured on a real 44-job corpus: all 44 parse and 211 of 216 nodes map, the only refusal being a site-specific custom component. Nothing is written to your workspace until you save, so a job that translates badly costs a closed tab.
- **The import report says what still needs a person.** A node count on its own would suggest a working pipeline, so the report leads with how many components actually translated, then lists everything unresolved. Encrypted Studio passwords arrive as `${ENV:...}` placeholders instead of guesses. Connections stored outside the job file are named, so you can fill them in or point the node at a saved connection. tMap outputs computed by Java are listed column by column with the expression to rewrite as SQL. A component with no Duckle equivalent is imported as a labelled placeholder, so the shape of the job survives rather than quietly losing a step.
- **Convert a whole repository at once, from the terminal.** Importing through a file dialog is the right shape for trying Duckle and the wrong shape for leaving another tool, because nobody has one job - they have several hundred in a checkout. `duckle-runner import <dir>` walks the tree, converts every job it finds, and mirrors the folder layout under `--out` so two jobs that share a name cannot overwrite each other. Measured on a real 125-file corpus: 42 files hold a job and 83 do not (routines, contexts and SQL templates share the extension), all 42 convert with none failing, and exactly one component across the whole corpus has no equivalent - a site-specific custom one. Everything else still to resolve is credentials that were never in the job files to begin with: 119 encrypted passwords and 75 connections defined outside the job. The closing tally lists unmapped components by how often they appear, which is both the answer to "is this migration viable" and the shortest path to finishing it. `--json` for a script, `--strict` to fail a CI job.
- **A file that was never a job is skipped, not counted.** The extension is shared by routines, contexts and SQL pattern templates. Parsing those yields an empty pipeline, and counting an empty pipeline as a converted job inflates the only number anyone reads - on the corpus above it turned 42 real conversions into a reported 82. They are now reported separately and no empty pipeline is written. The same rule applies in the other direction: a routine is Java source whose javadoc breaks any XML reader, so it is skipped rather than reported as a failed job, while a file that declares itself a job and then will not parse is still a failure.
- **Credentials are masked on token boundaries in exported SQL.** Redaction replaced a credential value wherever it appeared, so a password that was also a substring of an ordinary identifier corrupted the statement around it: a password of `prod` rewrote `production_report.parquet` as `${DUCKLE_PASSWORD}uction_report.parquet`. The secret itself was always protected; the damage was to everything else, and it mattered most when reading the Plan or SQL view to debug. Matching is now delimiter-aware, so `LOAD postgres` is left intact while a one-character password is still masked in `password=p'`. Deliberately no minimum length: a short password is still a password.

---

## What's new in v0.6.0

A multimodal AI data store, an importer for legacy visual ETL jobs, a chat
model you choose, and two geometry transforms that finally have the second
input they always needed.

- **Pixeltable, read and write (#223).** `src.pixeltable` reads a table, optionally filtered by a Pixeltable expression, a column subset and a limit; `snk.pixeltable` inserts into an existing table or creates one from the incoming rows. Versioned reads work by passing `myapp.media:3`. The exchange runs over Parquet on both legs - Pixeltable exports, Duckle ingests with `read_parquet`, and on the way back Duckle writes Parquet that `Table.insert` takes directly - so no rows are serialised one at a time. Pixeltable is a Python library, so the desktop app provisions a private Python for it with uv on first use; nothing is installed into your own environment.
- **Clip and Erase can now be wired up (#217, #218).** Both shipped in v0.5.9 as two-layer overlays, and the engine required the second layer, but the palette declared only one input - so the node could be placed and configured and never run. Both now offer a second input labelled **clip layer** / **erase layer**, like Spatial Join. Behaviour is unchanged: the second layer is still dissolved with `ST_Union_Agg` before the operation, attributes of the input layer are preserved, and features left with nothing are dropped. Thanks to @OmarMustaafa for reporting it twice with screenshots. A test now pins this contract for every component whose builder needs a second input, checked by removing a port and confirming it fails.
- **Choose the assistant's model, from 14 (#223 adjacent).** The setup step installed one hardcoded 1.5B model, which is right on a laptop and wrong on a workstation with a GPU. The catalogue now spans 469 MB to 9.9 GB - Qwen2.5 Coder 0.5B through 14B, Qwen3, Llama 3.2, Phi-3.5 Mini, Mistral 7B, Gemma 2 9B and DeepSeek Coder V2 Lite - each with its real download size and an honest note on what it needs. Every entry was checked to resolve before being offered, so the picker cannot hand you a file that 404s halfway through a multi-gigabyte download.
- **Import jobs from legacy visual ETL tools.** Reads the XML those jobs are stored as and produces a Duckle pipeline. Measured on a real 44-job corpus: all 44 parse and 211 of 216 nodes map, the only refusal being a site-specific custom component. Encrypted passwords become `${ENV:...}` placeholders rather than guesses, connections that live outside the job file are reported rather than silently half-imported, and anything with no equivalent is imported as a labelled placeholder so the shape of the job survives instead of quietly losing a step.
- **CI for your pipeline repo.** `duckle-runner` is now published as a release asset, and there are ready workflows for GitHub Actions and GitLab CI under `docs/ci/`. They gate every push on `duckle-runner validate`, which compiles pipelines to SQL without opening a source, writing a sink, or needing credentials or a network. This is the check that catches a column renamed in one commit and still referenced by another - they merge cleanly, and nothing else notices.
- **Node ids no longer collide.** Adding a folder or duplicating an item minted an id from the clock alone, so two of the same kind created in the same millisecond could share one. Both now use the same timestamp-plus-random scheme as everything else, which is what makes pipeline JSON safe to merge across branches.

Full notes: see the [v0.6.0 release](https://github.com/slothflowlabs/duckle/releases/tag/v0.6.0).

---

## What's new in v0.5.10

Power mode, context layering, and an Oracle extract that is now faster than
python-oracledb with pyarrow on the same table.

- **Power mode (Settings -> Power mode).** Two throughput settings per workspace. **Pipelines at once** caps how many run together; the placeholder shows the machine's core count. **Spill folder** points DuckDB's spill files at a bigger or faster disk. Only the lever with a measurement behind it is offered: independent pipelines scaled about 3.8x across 8 concurrent processes on a 20-core box, while splitting a single pipeline across processes measured *slower* (72ms to 123ms at 8-way), so there is deliberately no option for it. Each concurrent run gets its own memory limit and its own DuckDB process, so N at once needs roughly N times the memory, and the panel says so.
- **Scheduled runs have a ceiling.** Every schedule that came due in the same tick fired at once, so ten due at midnight meant ten pipelines each sized for the whole machine. They are now bounded, by power mode where it is set and by a sane default otherwise. The headless `duckle serve` honours the same setting, so desktop and server agree.
- **Contexts can be layered (#204).** A context can declare a **Layer**; higher layers override lower ones. A shared base plus a per-environment override is now expressible directly: give the base layer 0 and the environment a higher number, and the override applies quietly. Previously all contexts merged flat in repo order, so every intended override looked like a collision and had to be resolved by hand. Only two contexts on the *same* layer defining the same name are still reported, because nothing there says which should win. Workspaces that set no layers merge exactly as before.
- **Oracle extracts beat python-oracledb (#221).** Three changes, each measured on a 1,466,723-row x 236-column table with the same query and SNAPPY on both sides:
  - Unconstrained `NUMBER` columns are now **measured before the write instead of typed after it**. Those columns have no declared width, so they used to travel as text and be typed by a pass over the finished Parquet - which is exactly the pass a direct write skips, meaning one such column forced a whole second pass over every column. The ambiguous columns are now read on their own first (about 2s for 4 of 236), their real widths pin the schema, and the file is written once. Both reads share one snapshot via a read-only transaction, so they cannot disagree.
  - The driver no longer rebuilds an owned row per fetch. `ResultSet<Row>` reconstructs every value in the row; on this table that was 346 million reconstructions, measured at 11.5s of a 42.7s fetch against a 31.2s floor.
  - Scaled `NUMBER` values no longer allocate a string per cell while being rescaled - 88 million allocations per run on this shape.

  Together: **100.7s to 65.0s** in the shape reported on #221, against **68.6s** for python-oracledb with pyarrow doing the same job on the same machine. With column types already pinned it is about 56.7s. Output was verified against pyarrow's: identical row counts, and equal sums, hashes, ranges and null counts across every column type. Worth noting that python-oracledb maps an unconstrained `NUMBER` to `DOUBLE`, which cannot hold the 24 significant digits one test column carries; Duckle types it exactly, so the comparison is not quite like for like and not in our favour.
- **A direct Parquet write no longer produces string columns (#221).** With **Write directly from the source** enabled on a table containing any bare `NUMBER`, those columns were written as text while the run reported success. The source now declines the shortcut when a column cannot be typed before the write, and says so in the run log. Anyone who enabled that toggle on v0.5.9 against such a table should re-check the output.
- **Concurrent runs no longer fight over spill files.** DuckDB's default spill location is already per-run, but setting a shared spill folder made every run share one - which reads as a flaky run rather than a bug: across three trials of four concurrent spilling queries, a shared folder lost 3 of 12 runs to a segfault or a delete failure, private folders lost 0 of 12. Each run now spills into its own subfolder.

Full notes: see the [v0.5.10 release](https://github.com/slothflowlabs/duckle/releases/tag/v0.5.10).

---

## Roadmap

A complete planned-component breakdown lives in [`docs/roadmap.md`](docs/roadmap.md). Highlights:

- [ ] **Multi-shard Kinesis** and **Pulsar** streaming (Pulsar blocked on `protoc` at build time)
- [ ] **Apache ORC** read / write (blocked on the Arrow version conflict between `orc-rust` and our workspace pin)
- [x] **SFTP** source (shipped - `russh` + `russh-sftp` on the ring backend, password / key auth, host-fingerprint pin)
- [ ] **OAuth-heavy SaaS** (Google Sheets, Excel Online, full Salesforce OAuth, Gmail / O365 IMAP)
- [ ] **Embedded Python / Rust** code stages (current code.* family: SQL, Shell, JavaScript, WebAssembly all ship)
- [ ] **Hosted documentation site**
- [ ] **Plugin marketplace** via the connector SDK
- [ ] **In-process Native engine** - a Rust streaming / incremental executor as an alternative to shelling out to the DuckDB CLI

---

## Contributing

Contributions, issues, and ideas are welcome. Duckle is young and there is a lot of green field. Open an issue to discuss a change before a large PR, match the existing code style, and keep changes focused. Run `cargo test` and `npm --prefix frontend run build` before submitting. See [CONTRIBUTING.md](CONTRIBUTING.md).

---

## Contributors

Thanks goes to these wonderful people who contribute to Duckle ([emoji key](https://allcontributors.org/docs/en/emoji-key)):

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tbody>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/mitslabo"><img src="https://avatars.githubusercontent.com/u/176633224?v=4?s=100" width="100px;" alt="mits"/><br /><sub><b>mits</b></sub></a><br /><a href="#infra-mitslabo" title="Infrastructure (Hosting, Build-Tools, etc)">🚇</a> <a href="https://github.com/slothflowlabs/duckle/commits?author=mitslabo" title="Tests">⚠️</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/ABChristian"><img src="https://avatars.githubusercontent.com/u/4749931?v=4?s=100" width="100px;" alt="Christian"/><br /><sub><b>Christian</b></sub></a><br /><a href="#ideas-ABChristian" title="Ideas, Planning, & Feedback">🤔</a> <a href="https://github.com/slothflowlabs/duckle/commits?author=ABChristian" title="Tests">⚠️</a> <a href="https://github.com/slothflowlabs/duckle/commits?author=ABChristian" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/gmacc00"><img src="https://avatars.githubusercontent.com/u/46499110?v=4?s=100" width="100px;" alt="gmacc00"/><br /><sub><b>gmacc00</b></sub></a><br /><a href="#infra-gmacc00" title="Infrastructure (Hosting, Build-Tools, etc)">🚇</a> <a href="https://github.com/slothflowlabs/duckle/commits?author=gmacc00" title="Tests">⚠️</a> <a href="https://github.com/slothflowlabs/duckle/commits?author=gmacc00" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/stephaneheckel"><img src="https://avatars.githubusercontent.com/u/206326846?v=4?s=100" width="100px;" alt="Stéphane Heckel"/><br /><sub><b>Stéphane Heckel</b></sub></a><br /><a href="#infra-stephaneheckel" title="Infrastructure (Hosting, Build-Tools, etc)">🚇</a> <a href="https://github.com/slothflowlabs/duckle/commits?author=stephaneheckel" title="Tests">⚠️</a> <a href="https://github.com/slothflowlabs/duckle/commits?author=stephaneheckel" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/ssnowball"><img src="https://avatars.githubusercontent.com/u/10828099?v=4?s=100" width="100px;" alt="Steven Snowball"/><br /><sub><b>Steven Snowball</b></sub></a><br /><a href="#infra-ssnowball" title="Infrastructure (Hosting, Build-Tools, etc)">🚇</a> <a href="https://github.com/slothflowlabs/duckle/commits?author=ssnowball" title="Tests">⚠️</a> <a href="https://github.com/slothflowlabs/duckle/commits?author=ssnowball" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/Pian0610"><img src="https://avatars.githubusercontent.com/u/107343201?v=4?s=100" width="100px;" alt="Suffian0610"/><br /><sub><b>Suffian0610</b></sub></a><br /><a href="#infra-Pian0610" title="Infrastructure (Hosting, Build-Tools, etc)">🚇</a> <a href="https://github.com/slothflowlabs/duckle/commits?author=Pian0610" title="Tests">⚠️</a> <a href="https://github.com/slothflowlabs/duckle/commits?author=Pian0610" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/add944"><img src="https://avatars.githubusercontent.com/u/288381564?v=4?s=100" width="100px;" alt="add944"/><br /><sub><b>add944</b></sub></a><br /><a href="#infra-add944" title="Infrastructure (Hosting, Build-Tools, etc)">🚇</a> <a href="https://github.com/slothflowlabs/duckle/commits?author=add944" title="Tests">⚠️</a> <a href="https://github.com/slothflowlabs/duckle/commits?author=add944" title="Code">💻</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/KNP-BI"><img src="https://avatars.githubusercontent.com/u/73139861?v=4?s=100" width="100px;" alt="KNP-BI"/><br /><sub><b>KNP-BI</b></sub></a><br /><a href="#infra-KNP-BI" title="Infrastructure (Hosting, Build-Tools, etc)">🚇</a> <a href="https://github.com/slothflowlabs/duckle/commits?author=KNP-BI" title="Tests">⚠️</a> <a href="https://github.com/slothflowlabs/duckle/commits?author=KNP-BI" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://www.linkedin.com/in/riwesley/"><img src="https://avatars.githubusercontent.com/u/13156216?v=4?s=100" width="100px;" alt="Richard Wesley"/><br /><sub><b>Richard Wesley</b></sub></a><br /><a href="#infra-hawkfish" title="Infrastructure (Hosting, Build-Tools, etc)">🚇</a> <a href="https://github.com/slothflowlabs/duckle/commits?author=hawkfish" title="Tests">⚠️</a> <a href="https://github.com/slothflowlabs/duckle/commits?author=hawkfish" title="Code">💻</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/micha9ski"><img src="https://avatars.githubusercontent.com/u/200447708?v=4?s=100" width="100px;" alt="micha9ski"/><br /><sub><b>micha9ski</b></sub></a><br /><a href="#infra-micha9ski" title="Infrastructure (Hosting, Build-Tools, etc)">🚇</a> <a href="https://github.com/slothflowlabs/duckle/commits?author=micha9ski" title="Tests">⚠️</a> <a href="https://github.com/slothflowlabs/duckle/commits?author=micha9ski" title="Code">💻</a></td>
    </tr>
  </tbody>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

This project follows the [all-contributors](https://github.com/all-contributors/all-contributors) specification. Contributions of any kind - code, docs, design, bug reports, ideas - are welcome and recognized here. Comment on any issue or PR with `@all-contributors please add @name for code, doc` and the bot opens a PR adding them.

---

## License

Licensed under either of **MIT** or **Apache-2.0** at your option.

---

<div align="center">
<sub>Built with Rust, Tauri, React, and DuckDB by <a href="https://github.com/slothflowlabs">SlothFlowLabs</a></sub>
</div>

<!-- GitHub topics, as actually set on the repo. local-first and desktop-app were deliberately removed: they framed Duckle as a laptop tool, which is the read this copy exists to avoid. Keep this list in step with the repo settings.
     cdc, connectors, data-engineering, data-integration, data-orchestration, data-pipeline, data-quality, data-transformation, dbt, duckdb, elt, etl, kubernetes, lakehouse, low-code, mcp, no-code, open-source, reverse-etl, self-hosted -->
