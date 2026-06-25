// Generates src/workflow-ui/brand-icons.generated.ts: a base-name -> BrandIcon
// map of FULL-COLOUR connector logos used in the palette / node cards / quick-add.
//
// Two colour sources (build-time only; only the resolved markup is inlined, so
// the app bundle carries no icon-library dependency):
//   1. gilbarbara/logos (svgporn), CC0 - true multi-colour original logos, fetched
//      from jsdelivr. Stored as { svg } and rendered as an <img> data-URI.
//   2. simple-icons (+ legacy v9 for trademark-removed enterprise marks) - a
//      single-path mark tinted with the brand's official colour, for brands
//      gilbarbara doesn't carry. Stored as { path, color }.
// Anything in neither falls back to a generic lucide icon at render time.
//
// Run: node scripts/gen-brand-icons.mjs   (needs network for the gilbarbara CDN)
import * as si from 'simple-icons';
import * as siLegacy from 'si-legacy';
import { writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const __dir = dirname(fileURLToPath(import.meta.url));
const GH = 'https://cdn.jsdelivr.net/gh/gilbarbara/logos@main/logos';

// base name -> gilbarbara/logos slug (full multi-colour). Prefer the square
// "-icon" variant where one exists so marks sit evenly in the list.
const GB = {
    postgres: 'postgresql',
    pgvector: 'postgresql',
    mysql: 'mysql',
    mariadb: 'mariadb',
    oracle: 'oracle',
    db2: 'ibm',
    sqlite: 'sqlite',
    snowflake: 'snowflake-icon',
    redshift: 'aws-redshift',
    synapse: 'microsoft-azure',
    azureblob: 'microsoft-azure',
    eventhubs: 'microsoft-azure',
    s3: 'aws-s3',
    gcs: 'google-cloud',
    pubsub: 'google-cloud',
    r2: 'cloudflare',
    kafka: 'kafka-icon',
    nats: 'nats',
    rabbit: 'rabbitmq-icon',
    kinesis: 'aws-kinesis',
    dynamodb: 'aws-dynamodb',
    mongodb: 'mongodb-icon',
    cassandra: 'cassandra',
    redis: 'redis',
    elastic: 'elasticsearch',
    opensearch: 'opensearch',
    couchdb: 'couchdb',
    qdrant: 'qdrant',
    milvus: 'milvus',
    pinecone: 'pinecone',
    chroma: 'chroma',
    orc: 'apache',
    graphql: 'graphql',
    dbt: 'dbt',
    git: 'git-icon',
    github: 'github-icon',
    gitlab: 'gitlab',
    salesforce: 'salesforce',
    hubspot: 'hubspot',
    zendesk: 'zendesk',
    intercom: 'intercom',
    stripe: 'stripe',
    xero: 'xero',
    shopify: 'shopify',
    notion: 'notion',
    airtable: 'airtable',
    asana: 'asana',
    trello: 'trello',
    monday: 'monday',
    linear: 'linear',
    jira: 'jira',
    mailchimp: 'mailchimp',
    sendgrid: 'sendgrid',
    segment: 'segment',
    slack: 'slack-icon',
    discord: 'discord-icon',
    telegram: 'telegram',
    twilio: 'twilio',
    // pipedrive: only a wide wordmark exists (no square mark in either source),
    // so it falls back to a generic lucide icon rather than a tiny strip.
};

// base name -> simple-icons slug (single mark, tinted with brand colour), for
// brands gilbarbara doesn't carry.
const SI = {
    sqlserver: 'microsoftsqlserver',
    bigquery: 'googlebigquery',
    excel: 'microsoftexcel',
    'excel-online': 'microsoftexcel',
    gsheets: 'googlesheets',
    databricks: 'databricks',
    clickhouse: 'clickhouse',
    cockroach: 'cockroachlabs',
    pulsar: 'apachepulsar',
    duckdb: 'duckdb',
    ducklake: 'duckdb',
    quack: 'duckdb',
    minio: 'minio',
    b2: 'backblaze',
    scylla: 'scylladb',
    avro: 'apacheavro',
    parquet: 'apacheparquet',
    delta: 'databricks',
    spatial: 'geopandas',
    quickbooks: 'quickbooks',
    clickup: 'clickup',
};

// Custom raw-SVG logos for brands neither gilbarbara nor simple-icons
// carry (fetched from the vendor and embedded). Applied last, with
// precedence over the GB/SI results.
const CUSTOM = {
    lancedb: { png: "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", title: 'LanceDB' },
    vortex: { png: "iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAAFIklEQVR4nM1XXUiUaRR+ZhwVHYfJoaxmdQvK1ZgVoYJ2x7oI3ElvNEqILtwkvfAmo+6CppuJItaLWAJjNagBJzAxSRF1/YNSL0qlqTTXUmfVSTMrm52sLuYs5/Al4/w51gb7wPt9M+/fOe/5ec73qhA9vgfwCwALgGwA3wHQKmNeADMAHgJoB/AngL/xHyEbwB8AZgFQlG1WWcNrvxh6AJUA/lmD4MDGa39T9loTfgRw/ysEB7b7yp5R4SfFnxE3PXLkCOXk5KxFiRll74gwRSOcW0tLC7W1ta3VEjOKjGWoA3xuB2CMpGF8fDy2bNkCo9GI8fHxsPN0Oh0MBkNgt1GRoQ+lgBXAztVMZLPZUFtbK5s/e/ZsuV+r1eLixYvIz8/Htm3b0NPTg2PHjoXaYqciawWyALz3N1dKSgpdunQpyM9dXV3U0dFBr169osOHD0tfZmYm9ff304sXL+jGjRs0OztLjx49ouzs7HCueK/IXEZ14KSkpCTq6emh6elpMhqN0qfRaOjJkyd069YtevPmDWVlZVFBQQHNzc1Rb2+vCPd6vVRbW0sGg2G1eGCZglQA86EmsRUmJiaovb1dhCcnJ5Pb7Sa73U6vX78WgUtLSyLw9u3btLCwQCdOnIg2IFlmKsdAHoD1CIHc3Fz09vbCbDbDarVCr9dLEC4uLiI5ORkFBQXiZ+47ePAgqqqq0NTUhHXr1iEmJgarYL0iG3XhtLRareTxeKi6ulpOWlVVJeZOT0+nkpIScQHPO3PmDI2NjdHLly8lNthtTqeTWltb6ezZs6RSqcJZoU4FYBTAD6FUjIuLQ3Nzs6Sc0+nE0aNHMTk5iYyMDHz69GnF3ISEBOkvLy9HYmKiNLaSz+dDUVGRWC0E/uKHJ5KvOAD5RHfu3KErV65QaWlp2Ll8WgZb4e3btzQ/P08ul4sePHhADQ0NZLFYgmqFxq+khoTb7ZYTdHZ2oqKiAteuXQs79969e/j48SOuX7+OmzdvYvv27di6dau8N2/ejOnp6cAlifzw+WuVl5dHQ0ND1NTURJcvX6aTJ09Sfn7+csSvxv+cBZyKu3fvjiYTfBqFFJatoFar0dfXJ2xmsViQlJQk/uV+jUaD1FTO2mDExsbiwoULctLu7m7U19djz549mJubi2Rgb9gg3LRpE1paWsSkCwsLEkT8ttvtcLlc8psD7DNSUlLw/PlzUdjhcGDXrl1C1YcOHQoK2MAgrAtlHiaeU6dOiemZEZ8+fSpUy2TD6cZjPK+wsJBOnz5N+/btkzGbzUYfPnyQ1GVXrEJMdaxAWTTMFRMTQxs2bKCrV69KpHN8nDt3jnw+n/znfmZJrVYrNeTdu3dUU1NDZrM50r5lEanYv+l0OjkV1wB+d3d3S7pVVlbK2+Fw0PDwMKnVaoqLi5Px0dHRSDVBqBjhipF/M5lMNDAwIC7gk3J9GBwcpB07dkjWcL7zB0pnZ+fymrS0NKmKXEcSEhLCFiO1osDvAJZCRQnzPdcDDriOjg4UFxdLrd+/fz9GRkZgMpng9XqxcePGFXk+NTUlAdjY2CgZFIAlReYK8BdwkKbnz58X87ILDhw4QMePH18xzuz4+PFjunv3LpWVlUVbCVlWEPQABkL5PkIxkYLT3NxMer0+WuGD4T7JFgH8yuzrr5XH4wERrwuGSqUSvmBeCFNsAsF7FyuywuLnaL+MuTHl7t27N5q5M8re//+LyWfolWvV117NKr/kagY/fNPLKRejaPFNruf/AkPP8xrItPq7AAAAAElFTkSuQmCC", title: 'Vortex' },
    motherduck: { svg: "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"190\" height=\"190\" viewBox=\"-4.85 -33.25 190 190\"><path d=\"M88.8 11.6s17 56.2 18.2 59.2c1.2 3.1 4.2 5.9 7.9 7 3.7 1.1 7.8.2 10.4-1.8 2.6-2 46.5-40.9 46.5-40.9.6-.5 2.2-2.2 2.6-4.7.5-3.4-1.6-7-4.8-8.3-2.6-1-5-.3-5.7 0-1.1.5-3.1 1.2-5.6 1.3-2 .1-3.5-.3-4.3-.5-6.3-1.8-10.5-7.6-10.5-13.9-.1-3.9-2.7-7.5-6.6-8.6-4-1.1-8.1.6-10.2 3.9-3.3 5.4-9.8 8.1-16.2 6.3-3.4-1-6.2-3.1-8-5.8-.5-.6-2.1-2.6-4.9-3.1-3.4-.6-7.1 1.4-8.5 4.5-1.1 2.4-.5 4.6-.3 5.4ZM9.3 86.3s47.7 34.1 50.6 35.8c2.8 1.7 6.9 2.1 10.5.7 3.6-1.4 6.3-4.6 7.1-7.8.9-3.2 11.8-60.8 11.8-60.8.1-.8.4-3.1-.8-5.3-1.7-3-5.5-4.6-8.9-3.6-2.7.8-4.1 2.8-4.5 3.5-.6 1.1-1.7 2.9-3.7 4.5-1.5 1.2-3 1.9-3.7 2.2-6.1 2.5-12.9.4-16.8-4.6-2.4-3.1-6.7-4.3-10.5-2.8s-6 5.4-5.7 9.3c.7 6.2-2.8 12.4-9 14.9-3.3 1.3-6.8 1.3-9.9.3-.7-.2-3.2-.8-5.7.6C7 74.8 5.4 78.6 6.2 82c.6 2.3 2.4 3.8 3.1 4.3Z\" fill=\"#FF9538\"/></svg>", title: 'MotherDuck' },
};

// --- simple-icons index (legacy first, current overwrites) ---
const bySlug = new Map();
for (const v of Object.values(siLegacy)) {
    if (v && typeof v === 'object' && v.slug && v.path) bySlug.set(v.slug, v);
}
for (const v of Object.values(si)) {
    if (v && typeof v === 'object' && v.slug && v.path) bySlug.set(v.slug, v);
}

// simple-icons slug overrides for the wide-wordmark fallback (where the slug
// isn't just the base name).
const SI_FALLBACK = {
    db2: 'ibm',
    cassandra: 'apachecassandra',
    couchdb: 'apachecouchdb',
    nats: 'natsdotio',
    rabbit: 'rabbitmq',
    kafka: 'apachekafka',
    orc: 'apache',
};
const siFor = base => bySlug.get(SI_FALLBACK[base] || base);

// Trim an svgporn SVG down to just its <svg>...</svg> markup.
function cleanSvg(s) {
    const i = s.indexOf('<svg');
    const j = s.lastIndexOf('</svg>');
    if (i < 0 || j < 0) return null;
    return s.slice(i, j + 6).replace(/\r?\n\s*/g, ' ').replace(/<!--.*?-->/g, '').trim();
}

// Aspect ratio (w/h) of an SVG's viewBox; null if unknown.
function ratioOf(svg) {
    const m = svg.match(/viewBox="([\d.\- ]+)"/);
    if (!m) return null;
    const p = m[1].trim().split(/\s+/).map(Number);
    return p[2] && p[3] ? p[2] / p[3] : null;
}

const out = {};
const missing = [];

// Discover which gilbarbara slugs exist so we can prefer the square "-icon"
// logomark variant over a wide wordmark.
const flat = await (
    await fetch('https://data.jsdelivr.com/v1/packages/gh/gilbarbara/logos@main?structure=flat')
).json();
const available = new Set(
    flat.files
        .map(f => f.name)
        .filter(n => /^\/logos\/.*\.svg$/.test(n))
        .map(n => n.replace('/logos/', '').replace('.svg', '')),
);
function squareSlug(slug) {
    const stem = slug.replace(/-icon$/, '');
    for (const c of [`${stem}-icon`, slug]) if (available.has(c)) return c;
    return available.has(slug) ? slug : null;
}

// 1. gilbarbara multi-colour, preferring the square logomark. A logo that is
// still very wide/tall after that (e.g. a text-only wordmark) reads as a tiny
// strip in a square slot, so fall back to the square 24x24 simple-icons mark
// tinted with the brand colour.
const fetched = await Promise.all(
    Object.entries(GB).map(async ([base, slug]) => {
        const pick = squareSlug(slug);
        if (!pick) return [base, slug, null];
        try {
            const r = await fetch(`${GH}/${pick}.svg`);
            return [base, pick, r.ok ? cleanSvg(await r.text()) : null];
        } catch {
            return [base, pick, null];
        }
    }),
);
for (const [base, slug, svg] of fetched) {
    const r = svg ? ratioOf(svg) : null;
    const squareEnough = r !== null && r >= 0.45 && r <= 2.0;
    if (svg && squareEnough) {
        out[base] = { svg, title: slug };
        continue;
    }
    const icon = siFor(base); // square 24x24 fallback for wide/missing marks
    if (icon) out[base] = { path: icon.path, color: '#' + icon.hex, title: icon.title };
    else if (svg) out[base] = { svg, title: slug }; // wide, but better than nothing
    else missing.push(`gilbarbara ${base} -> ${slug}`);
}

// 2. simple-icons tinted fallback for brands gilbarbara doesn't carry at all.
for (const [base, slug] of Object.entries(SI)) {
    if (out[base]) continue;
    const icon = bySlug.get(slug);
    if (icon) out[base] = { path: icon.path, color: '#' + icon.hex, title: icon.title };
    else missing.push(`simple-icons ${base} -> ${slug}`);
}

for (const [base, v] of Object.entries(CUSTOM)) out[base] = v;

const header =
    '// AUTO-GENERATED by scripts/gen-brand-icons.mjs. Do not edit by hand.\n' +
    '// Full-colour connector logos. { svg } = gilbarbara/logos (rendered as an\n' +
    '// <img>); { path, color } = a simple-icons mark tinted with its brand colour.\n\n' +
    'export type BrandIcon =\n' +
    '    | { svg: string; title: string }\n' +
    '    | { png: string; title: string }\n' +
    '    | { path: string; color: string; title: string };\n\n' +
    'export const BRAND_ICONS: Record<string, BrandIcon> = ';
writeFileSync(
    join(__dir, '..', 'src', 'workflow-ui', 'brand-icons.generated.ts'),
    header + JSON.stringify(out, null, 2) + ';\n',
);

const svgCount = Object.values(out).filter(v => 'svg' in v).length;
const tintCount = Object.values(out).filter(v => 'path' in v).length;
console.log(`brand-icons: ${Object.keys(out).length} icons (${svgCount} colour SVG, ${tintCount} tinted)`);
if (missing.length) {
    console.log(`MISSING (${missing.length}) -> generic fallback:`);
    for (const m of missing) console.log('  ' + m);
}
