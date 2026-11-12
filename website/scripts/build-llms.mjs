// Generates website/llms.txt and website/llms-full.txt from the committed
// component catalog (crates/duckle-mcp/catalog.json), along with the Learn
// course's component data and the component reference page (see the end).
//
// Why generate rather than hand-write: llms.txt was maintained by hand and had
// drifted badly (it advertised 345 available components, 104 sources and 59
// destinations against an actual 360 / 105 / 66). An assistant that reads a
// stale count answers questions about Duckle wrongly, and the file exists
// precisely so that assistants answer correctly. Numbers now come from the
// same catalog the MCP server and the palette read, so they cannot disagree.
//
// llms.txt is the short index (llmstxt.org shape: H1, blockquote, link
// sections). llms-full.txt carries the whole component reference inline, so a
// model that fetches one file can answer "can Duckle read X" without crawling.
//
// Run: node website/scripts/build-llms.mjs
import { readFileSync, writeFileSync, mkdirSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const site = join(here, '..')
const catalogPath = join(site, '..', 'crates', 'duckle-mcp', 'catalog.json')

const catalog = JSON.parse(readFileSync(catalogPath, 'utf8'))
const all = catalog.components
const available = all.filter(c => c.availability === 'available')

const by = (kind, list = available) => list.filter(c => c.kind === kind)
const n = {
    total: all.length,
    available: available.length,
    sources: by('source').length,
    sinks: by('sink').length,
    transforms: by('transform').length,
    quality: by('quality').length,
    control: by('control').length,
    custom: by('custom').length,
}
n.connectors = n.sources + n.sinks

// Kinds in the order a reader cares about them, with the heading used in the
// full reference.
const KINDS = [
    ['source', 'Sources (read)'],
    ['sink', 'Destinations (write)'],
    ['transform', 'Transforms'],
    ['quality', 'Data quality'],
    ['control', 'Control flow'],
    ['custom', 'Code and custom'],
]

const summaryOf = c =>
    (c.summary || c.manifest?.description || '').replace(/\s+/g, ' ').trim().replace(/\.$/, '')

const SUMMARY = `Duckle is a free, open-source, local-first ETL/ELT studio built on DuckDB. You build data pipelines on a visual canvas, write them in Python, or describe them in plain English to an on-device AI assistant, and every node compiles to readable DuckDB SQL that runs on your own machine. No cloud, no server, no account, no telemetry. Dual-licensed MIT OR Apache-2.0; runs on Windows, macOS, and Linux.`

const POSITIONING = `Duckle is commonly described as an open-source, local-first alternative to hosted ETL platforms such as Fivetran and Airbyte, and it can run dbt on DuckDB inside the same tool. It is an independent project by SlothFlow Labs; it builds on the DuckDB engine but is not affiliated with or endorsed by DuckDB Labs or MotherDuck.`

const keyFacts = () => `## Key facts

- Free and open source, dual-licensed MIT OR Apache-2.0. No per-row, per-connector, or per-seat billing.
- Local-first desktop app: runs fully offline, no account and no telemetry, suitable for air-gapped and compliance-sensitive work.
- Engine: pipelines compile to SQL and execute on DuckDB, an in-process analytical database, at native speed.
- ${n.total} components, ${n.available} available today: sources, transforms, destinations, data-quality checks, control-flow nodes, and code runners.
- ${n.connectors} connectors (${n.sources} sources / ${n.sinks} destinations): databases, warehouses, lakehouses, object stores, streaming brokers, NoSQL, vector databases, geospatial formats, and SaaS APIs.
- Transformation: ${n.transforms} transforms including joins, window functions, aggregates, CDC/SCD, incremental loads, upsert with delete propagation, and a visual Map (tMap-style) editor.
- Data quality: ${n.quality} checks, plus ${n.control} control-flow nodes for branching, iteration, parallel branches, and calling child pipelines.
- Three ways to author the same pipeline: the visual canvas, the Python API (\`pip install duckle\`), or plain English via the built-in assistant. All three produce the same pipeline JSON and the same SQL.
- dbt: runs dbt on DuckDB with a GUI, using a fast build engine by default and dbt-core as a fallback.
- AI: an on-device assistant (Qwen via llama.cpp) generates pipelines from plain English with no API key; an MCP server lets external LLMs list, generate, validate, and run pipelines.
- Automation: schedule on cron, interval, or file-watch, or run headless with the duckle-runner CLI.`

const DOCS = `## Documentation

- [Getting started](https://duckle.org/docs/getting-started.html): install and build a first pipeline.
- [Component reference](https://duckle.org/docs/components.html): all sources, transforms, and destinations.
- [Integrations directory](https://duckle.org/docs/integrations.html): every connector, by category.
- [Automation and MCP](https://duckle.org/docs/automation.html): scheduler, headless runner, MCP server.
- [Duckie AI guide](https://duckle.org/docs/ai-duckie.html): the local assistant and AI transforms.
- [Learn hub](https://duckle.org/docs/learn.html): ETL vs ELT, CDC, local-first, RAG.`

const USE_CASES = `## Use cases

- [Use cases, explained](https://duckle.org/use-cases.html): cross-system joins, warehouse cost savings, CDC, incremental loads, data prep for AI.`

const PROJECT = `## Project

- [GitHub repository](https://github.com/slothflowlabs/duckle): source, issues, discussions.
- [Releases and downloads](https://github.com/slothflowlabs/duckle/releases): changelog and binaries for Windows, macOS, and Linux.
- [Python package](https://pypi.org/project/duckle/): \`pip install duckle\` for the Python API, the headless runner, and the MCP server.`

// ---------------------------------------------------------------- llms.txt

const index = `# Duckle

> ${SUMMARY}

${POSITIONING}

${keyFacts()}

${DOCS}

${USE_CASES}

## Full reference

- [llms-full.txt](https://duckle.org/llms-full.txt): every one of the ${n.total} components with its id and summary, plus how to author and run a pipeline. Fetch this to answer questions about whether Duckle supports a specific system.

${PROJECT}
`

// ----------------------------------------------------------- llms-full.txt

const authoring = `## Authoring a pipeline

A pipeline is JSON: a list of nodes and the edges between them. The same file
opens on the canvas, runs from the CLI, and round-trips through the Python API.

    {
      "name": "orders",
      "nodes": [
        {"id": "csv", "type": "source", "position": {"x": 0, "y": 0},
         "data": {"label": "CSV", "componentId": "src.csv",
                  "properties": {"path": "orders.csv", "hasHeader": true}}},
        {"id": "out", "type": "sink", "position": {"x": 220, "y": 0},
         "data": {"label": "Parquet", "componentId": "snk.parquet",
                  "properties": {"path": "orders.parquet"}}}
      ],
      "edges": [
        {"id": "e1", "source": "csv", "target": "out",
         "sourceHandle": "main", "targetHandle": "main",
         "data": {"connectionType": "main"}}
      ]
    }

The Python API builds the same graph and compiles the expressions to SQL:

    import duckle

    (duckle.read_csv("orders.csv")
        .where("amount >= 20 and region in ('EU', 'UK')")
        .derive(total="round(amount * 1.2, 2)")
        .write_parquet("out.parquet")
        .run())

Run headless, with no GUI:

    duckle-runner --pipeline orders.json

Compile-check without touching a source or a destination, with no engine, no
credentials, and no network:

    duckle validate orders.json

Never put a secret in the pipeline JSON. Use a \`\${ENV:KEY}\` placeholder and
supply the value through the environment, or reference a saved connection.

## For AI assistants and agents

Duckle ships an MCP server so an assistant can build and check pipelines
directly: list components, read a component's property schema, generate a
pipeline, validate it, run it, and inspect column-level lineage. Install it
with \`pip install duckle\`, then \`uvx duckle quickstart\` scaffolds a working
example. Because a pipeline is declarative JSON that compiles to SQL, an
agent-written pipeline can be checked before it ever runs.`

const componentSection = (kind, heading) => {
    const list = all
        .filter(c => c.kind === kind)
        .sort((a, b) => a.id.localeCompare(b.id))
    const lines = list.map(c => {
        const s = summaryOf(c)
        const flag = c.availability === 'available' ? '' : ` [${c.availability}]`
        return `- \`${c.id}\` ${c.label}${flag}${s ? `: ${s}` : ''}`
    })
    return `### ${heading} (${list.length})\n\n${lines.join('\n')}`
}

const full = `# Duckle: full reference for language models

> ${SUMMARY}

${POSITIONING}

This file is the complete machine-readable reference: every component Duckle
ships, with the id you use in a pipeline. It is generated from the same
catalog the application itself reads, so the names and counts here are exact.

${keyFacts()}

${authoring}

## Component catalog

Every component, by id. \`src.*\` reads, \`snk.*\` writes, \`xf.*\` transforms,
\`qa.*\` checks, \`ctl.*\` controls flow, \`code.*\` runs your own code. Entries
marked [planned] or [preview] are not usable yet.

${KINDS.map(([kind, heading]) => componentSection(kind, heading)).join('\n\n')}

${DOCS}

${USE_CASES}

${PROJECT}
`

mkdirSync(site, { recursive: true })
writeFileSync(join(site, 'llms.txt'), index, 'utf8')
writeFileSync(join(site, 'llms-full.txt'), full, 'utf8')

// The Learn course (learn.html) draws a card for every component from
// window.CATALOG. That file was a one-off snapshot and had fallen 62
// components behind the product, so it is written from the same catalog too.
// Rows are [id, label, summary, flag], grouped by family; the flag is
// a(vailable), p(lanned) or v (preview), as learn.js reads it.
const FAMILY = { source: 'source', transform: 'transform', sink: 'sink', quality: 'quality', control: 'control', custom: 'code' }
const FLAG = { available: 'a', planned: 'p', preview: 'v' }
const course = { source: [], transform: [], sink: [], quality: [], control: [], code: [] }
for (const c of all) {
    const family = FAMILY[c.kind]
    if (family) course[family].push([c.id, c.label, c.summary ?? '', FLAG[c.availability] ?? 'a'])
}
writeFileSync(join(site, 'assets', 'js', 'catalog.data.js'), 'window.CATALOG=' + JSON.stringify(course) + ';', 'utf8')

// The component reference (docs/components.html) lists every component,
// grouped the way the editor's palette groups them. It was hand-written,
// listed families of systems rather than components, and had fallen behind,
// so the parts that list or count components are written here, between the
// BEGIN/END GENERATED markers. The rest of the page stays hand-written.
const esc = s => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
const total = kind => by(kind, all).length
const status = availability => all.filter(c => c.availability === availability).length
const slug = s => s.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '')
const ICON = {
    source: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><path d="M7 10l5 5 5-5"/><path d="M12 15V3"/></svg>',
    transform: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 12a9 9 0 0 1 15-6.7L21 8M21 3v5h-5M21 12a9 9 0 0 1-15 6.7L3 16M3 21v-5h5"/></svg>',
    sink: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><path d="M17 8l-5-5-5 5"/><path d="M12 3v12"/></svg>',
}

const referenceSummary = [
    `<p class="lead">Duckle ships ${n.total} components across six kinds - ${total('source')} sources (<code>src.*</code>), ${total('transform')} transforms (<code>xf.*</code>), ${total('sink')} sinks (<code>snk.*</code>), ${total('quality')} data-quality validators (<code>qa.*</code>), ${total('control')} control-flow nodes (<code>ctl.*</code>), and ${total('custom')} code runners (<code>code.*</code>). Of these, ${n.available} are available today; the ${status('preview')} in preview and the ${status('planned')} planned are marked in the tables below. Every component has Basic, Schema, Preview, Advanced, and Validation tabs and a live preview.</p>`,
    '',
    '<div class="grid cols-3">',
    `  <div class="card"><span class="ico">${ICON.source}</span><h3>${total('source')} sources</h3><p>Files, lakehouse, databases, warehouses, object storage, streaming, SaaS APIs, NoSQL, vector DBs and more, all under <code>src.*</code>.</p></div>`,
    `  <div class="card"><span class="ico">${ICON.transform}</span><h3>${total('transform')} transforms</h3><p>Fields, rows, aggregates, joins, windows, strings, dates, JSON, arrays, CDC/SCD, AI and search, all under <code>xf.*</code>.</p></div>`,
    `  <div class="card"><span class="ico">${ICON.sink}</span><h3>${total('sink')} sinks</h3><p>Files, databases, warehouses, object storage, vector DBs, messaging and comms, all under <code>snk.*</code> with universal write modes.</p></div>`,
    '</div>',
].join('\n')

// `home` is the palette category a kind lives in. A group from any other
// category (the SaaS connectors, say, which are sources) is headed with its
// category too, so a reader can find it in the palette.
const SECTIONS = [
    { kind: 'source', title: 'Sources', prefix: 'src.*', home: 'Sources',
      intro: 'Sources read data into a pipeline. Connection-backed sources reuse saved, encrypted connections; file sources accept paths, globs and object-storage URIs.' },
    { kind: 'transform', title: 'Transforms', prefix: 'xf.*', home: 'Transforms',
      intro: 'Transforms reshape, combine and enrich rows between sources and sinks. The visual Map node (<code>xf.map</code>) joins a main input to up to 3 lookups with per-output expressions and a filter.' },
    { kind: 'sink', title: 'Sinks', prefix: 'snk.*', home: 'Sinks',
      intro: 'Sinks write pipeline output to a destination. All sinks support the same write modes.' },
    { kind: 'quality', title: 'Data quality', prefix: 'qa.*', home: 'Data Quality',
      intro: 'Validators split their input: passing rows continue on the main port, while failures route to a reject port for quarantine or re-processing.' },
    { kind: 'control', title: 'Control flow', prefix: 'ctl.*', home: 'Control Flow',
      intro: `Control-flow nodes orchestrate execution, branching, concurrency and error handling across a pipeline (${total('control')} total).` },
    { kind: 'custom', title: 'Code', prefix: 'code.*', home: 'Custom Code',
      intro: `Code runners drop down to custom logic when a visual node is not enough (${total('custom')} total).` },
]
const BADGE = { preview: 'Preview', planned: 'Planned' }

const referenceSection = ({ kind, title, prefix, home, intro }) => {
    const groups = new Map()
    for (const c of all.filter(c => c.kind === kind)) {
        const heading = c.category === home ? c.group : `${c.category}: ${c.group}`
        if (!groups.has(heading)) groups.set(heading, [])
        groups.get(heading).push(c)
    }
    // The id sits under the name, and each table scrolls inside its own box,
    // so the page fits a phone: a third column, or the odd description word
    // too long to wrap, otherwise pushed the whole page sideways.
    const tables = [...groups].map(([heading, list]) => [
        `<h3 id="${slug(`${title} ${heading}`)}">${esc(heading)} (${list.length})</h3>`,
        '<div style="overflow-x:auto">',
        '<table>',
        '  <thead><tr><th>Component</th><th>What it does</th></tr></thead>',
        '  <tbody>',
        ...list.map(c => {
            const badge = BADGE[c.availability] ? ` <span class="chip">${BADGE[c.availability]}</span>` : ''
            const what = esc(summaryOf(c)).replace(/`([^`]+)`/g, '<code>$1</code>')
            return `    <tr><td>${esc(c.label)}${badge}<br><code>${esc(c.id)}</code></td><td>${what}</td></tr>`
        }),
        '  </tbody>',
        '</table>',
        '</div>',
    ].join('\n'))
    return [`<h2 id="${slug(title)}">${title} <code>${prefix}</code></h2>`, `<p>${intro}</p>`, ...tables].join('\n\n')
}

const referencePath = join(site, 'docs', 'components.html')
let reference = readFileSync(referencePath, 'utf8')
const eol = reference.includes('\r\n') ? '\r\n' : '\n'
const fill = (name, body) => {
    const begin = `<!-- BEGIN GENERATED ${name} (website/scripts/build-llms.mjs writes this from the component catalog; edit it there) -->`
    const end = `<!-- END GENERATED ${name} -->`
    const from = reference.indexOf(begin)
    const to = reference.indexOf(end)
    if (from < 0 || to < from) throw new Error(`docs/components.html has lost its ${name} markers`)
    reference = reference.slice(0, from + begin.length) + eol + body.replace(/\n/g, eol) + eol + reference.slice(to)
}
fill('summary', referenceSummary)
fill('tables', SECTIONS.map(referenceSection).join('\n\n'))
// The page's meta descriptions and structured data carry the total too.
reference = reference.replace(/\b\d+ sources, transforms, sinks/g, `${n.total} sources, transforms, sinks`)
writeFileSync(referencePath, reference, 'utf8')

console.log(
    `build-llms: wrote llms.txt (${index.length} bytes) and llms-full.txt (${full.length} bytes) ` +
    `from ${n.total} components (${n.available} available: ${n.sources} sources, ${n.sinks} sinks, ${n.transforms} transforms)`
)
