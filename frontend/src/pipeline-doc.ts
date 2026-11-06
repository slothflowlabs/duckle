// Generated pipeline documentation, as Talend's "Generate Doc as HTML": one
// self-contained page a person can read without Duckle, describing every step
// in the order it runs, what it is set to and the columns it produces, with a
// diagram, the canvas notes, and what the pipeline needs from outside.
//
// The page is meant to be passed around, so nothing secret goes into it. A
// value is withheld when the form masks its field, when its key is one the
// engine calls a credential, or when it is an auth header; a password inside
// a URL or a connection string is cut out of the text; secret context
// variables and parameters are named without their values; and a saved
// connection is given by name and kind only. A secret written as a reference,
// ${ENV:NAME}, is shown, because it says where the value comes from.
// Compiled SQL is left out on purpose: it is built from resolved values.
import type { Edge, Node } from '@xyflow/react';
import type { DuckleNodeData, NoteData } from './pipeline-types';
import type { ConnectionPayload, ContextPayload, RepoItem } from './repo-types';
import type { Field } from './workflow-ui/fields/types';
import { getManifest } from './workflow-ui/fields/component-manifests';
import { isFieldVisible } from './workflow-ui/fields/visibility';
import { isSecretField } from './workflow-ui/fields/PrimitiveFields';
import { resolveOutputSchema } from './schema-resolve';
import { deriveNodeSubtitle } from './node-subtitle';
import { discoverParams, pipelineRunFields } from './run-resolve';
import { isSecretPropKey } from './secret-keys';

export type DocInput = {
    /** The pipeline's name, as the project tree shows it. */
    name: string;
    /** The pipeline document as it is saved: nodes, edges, notes, parameters. */
    pipeline: unknown;
    repo: RepoItem[];
    /** The context whose values the page shows for the variables the pipeline reads. */
    contextId?: string | null;
    generatedAt: Date;
};

type Step = Node<DuckleNodeData>;

const WITHHELD = '<span class="withheld">secret, not included</span>';

const esc = (s: unknown) =>
    String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c]!);

// A credential can sit inside an ordinary string, as the user:password of a URL
// or the password= of a connection string, under a key that looks harmless.
function scrub(text: string): string {
    return text
        .replace(/(\b[a-z][a-z0-9+.-]*:\/\/[^\s/:@]+):[^\s/@]+@/gi, '$1:(secret)@')
        .replace(/\b(password|passwd|pwd)\s*=\s*('[^']*'|"[^"]*"|[^\s;&]+)/gi, '$1=(secret)');
}

/** A key, header or pair name whose value is a credential. */
const sensitiveName = (name: string) =>
    isSecretPropKey(name) || /^(proxy-)?authorization$|^cookie$|api[-_]?key/i.test(name);

/** `${ENV:NAME}` is not the secret but where it comes from. */
const isReference = (v: unknown) => typeof v === 'string' && /^\$\{[^}]+\}$/.test(v.trim());

const isEmpty = (v: unknown) =>
    v === undefined || v === null || v === '' || (Array.isArray(v) && v.length === 0);

type Pair = { key: unknown; value: unknown };
const isPairList = (v: unknown): v is Pair[] =>
    Array.isArray(v) && v.every(e => !!e && typeof e === 'object' && 'key' in e && 'value' in e);

/** A list or object with every credential-keyed value replaced, for showing as JSON. */
function redact(value: unknown): unknown {
    if (Array.isArray(value)) return value.map(redact);
    if (!value || typeof value !== 'object') return value;
    const entries = Object.entries(value as Record<string, unknown>);
    // A {key, value} pair names its own credential in `key`.
    const pairName = entries.find(([k]) => k === 'key')?.[1];
    return Object.fromEntries(
        entries.map(([k, v]) => {
            const secret =
                sensitiveName(k) || (k === 'value' && typeof pairName === 'string' && sensitiveName(pairName));
            return [k, secret && !isReference(v) ? '(secret, not included)' : redact(v)];
        }),
    );
}

/** A value as the page shows it, with any credential inside it withheld. */
function shown(value: unknown): string {
    if (typeof value === 'boolean') return value ? 'Yes' : 'No';
    if (typeof value === 'number') return esc(value);
    if (typeof value === 'string') {
        const text = esc(scrub(value));
        return value.includes('\n') ? `<pre>${text}</pre>` : `<code>${text}</code>`;
    }
    if (isPairList(value)) {
        const rows = value
            .map(({ key, value: v }) => {
                const name = String(key ?? '');
                const cell = sensitiveName(name) && !isReference(v) ? WITHHELD : shown(v);
                return `<tr><td><code>${esc(scrub(name))}</code></td><td>${cell}</td></tr>`;
            })
            .join('');
        return `<table class="pairs">${rows}</table>`;
    }
    if (Array.isArray(value) && value.every(v => v === null || typeof v !== 'object')) {
        return value.map(shown).join(', ');
    }
    return `<pre>${esc(scrub(JSON.stringify(redact(value), null, 2)))}</pre>`;
}

function settingCell(key: string, value: unknown, field: Field | undefined, repo: RepoItem[]): string {
    if ((field && isSecretField(field)) || isSecretPropKey(key)) {
        return isReference(value) ? `<code>${esc(value)}</code>` : WITHHELD;
    }
    if (field?.kind === 'connection-ref') {
        const conn = repo.find(i => i.id === value && i.type === 'connection');
        return conn ? esc(conn.name) : `<span class="missing">missing connection ${esc(value)}</span>`;
    }
    const option = field?.options?.find(o => String(o.value) === String(value));
    return option ? esc(option.label) : shown(value);
}

/** The settings a step's form shows, under the form's labels, then any property no field declares. */
function settings(step: Step, repo: RepoItem[]): { label: string; html: string }[] {
    const props = step.data.properties ?? {};
    const manifest = getManifest(step.data.componentId);
    const rows: { label: string; html: string }[] = [];
    const declared = new Set<string>();
    for (const section of manifest?.sections ?? []) {
        for (const field of section.fields) {
            declared.add(field.key);
            // A hidden field's value is not what runs, so it is not documented.
            if (!isFieldVisible(field, manifest!, props)) continue;
            const value = props[field.key] !== undefined ? props[field.key] : field.defaultValue;
            if (!isEmpty(value)) rows.push({ label: field.label, html: settingCell(field.key, value, field, repo) });
        }
    }
    for (const [key, value] of Object.entries(props)) {
        if (!declared.has(key) && !isEmpty(value)) rows.push({ label: key, html: settingCell(key, value, undefined, repo) });
    }
    return rows;
}

/** The connections a step names, by id, through its visible connection fields. */
function connectionsOf(step: Step): string[] {
    const props = step.data.properties ?? {};
    const manifest = getManifest(step.data.componentId);
    if (!manifest) return [];
    return manifest.sections
        .flatMap(s => s.fields)
        .filter(f => f.kind === 'connection-ref' && isFieldVisible(f, manifest, props) && !isEmpty(props[f.key]))
        .map(f => String(props[f.key]));
}

/** Steps in the order they run: sources first, each after everything it reads from. */
function runOrder(steps: Step[], edges: Edge[]): Step[] {
    const byPlace = (a: Step, b: Step) => a.position.x - b.position.x || a.position.y - b.position.y;
    const ids = new Set(steps.map(s => s.id));
    const waiting = new Map(steps.map(s => [s.id, 0]));
    const next = new Map<string, string[]>();
    for (const e of edges) {
        if (!ids.has(e.source) || !ids.has(e.target)) continue;
        waiting.set(e.target, waiting.get(e.target)! + 1);
        next.set(e.source, [...(next.get(e.source) ?? []), e.target]);
    }
    const ready = steps.filter(s => waiting.get(s.id) === 0).sort(byPlace);
    const order: Step[] = [];
    while (ready.length) {
        const step = ready.shift()!;
        order.push(step);
        for (const id of next.get(step.id) ?? []) {
            waiting.set(id, waiting.get(id)! - 1);
            if (waiting.get(id) === 0) {
                ready.push(steps.find(s => s.id === id)!);
                ready.sort(byPlace);
            }
        }
    }
    // A cycle leaves steps unplaced; they follow, by position, rather than vanish.
    const placed = new Set(order.map(s => s.id));
    return [...order, ...steps.filter(s => !placed.has(s.id)).sort(byPlace)];
}

const kindOf = (step: Step) => step.type ?? getManifest(step.data.componentId)?.kind ?? 'transform';
const linkType = (e: Edge) => (e.data as { connectionType?: string } | undefined)?.connectionType ?? 'main';
const clip = (s: string, n: number) => (s.length > n ? s.slice(0, n - 1) + '…' : s);

function diagram(steps: Step[], edges: Edge[], notes: Node<NoteData>[]): string {
    const box = (n: Node, w: number, h: number) => ({
        x: n.position.x,
        y: n.position.y,
        w: n.measured?.width ?? w,
        h: n.measured?.height ?? h,
    });
    const boxes = new Map(steps.map(s => [s.id, box(s, 220, 110)]));
    const noteBoxes = notes.map(n => ({ note: n, ...box(n, 220, 80) }));
    const all = [...boxes.values(), ...noteBoxes];
    if (all.length === 0) return '';
    const minX = Math.min(...all.map(b => b.x)) - 30;
    const minY = Math.min(...all.map(b => b.y)) - 30;
    const width = Math.max(...all.map(b => b.x + b.w)) + 30 - minX;
    const height = Math.max(...all.map(b => b.y + b.h)) + 30 - minY;
    const links = edges
        .map(e => {
            const s = boxes.get(e.source);
            const t = boxes.get(e.target);
            if (!s || !t) return '';
            const [sx, sy, tx, ty] = [s.x + s.w, s.y + s.h / 2, t.x, t.y + t.h / 2];
            const dx = Math.max(40, Math.abs(tx - sx) / 2);
            const type = linkType(e);
            return `<path class="link link-${esc(type)}" d="M${sx} ${sy} C${sx + dx} ${sy}, ${tx - dx} ${ty}, ${tx} ${ty}"/>`;
        })
        .join('');
    const stepShapes = steps
        .map(s => {
            const b = boxes.get(s.id)!;
            const comp = getManifest(s.data.componentId)?.label ?? s.data.componentId ?? '';
            return (
                `<g${s.data.disabled ? ' class="off"' : ''}>` +
                `<rect class="node node-${esc(kindOf(s))}" x="${b.x}" y="${b.y}" width="${b.w}" height="${b.h}" rx="8"/>` +
                `<text class="node-label" x="${b.x + 14}" y="${b.y + 30}">${esc(clip(s.data.label || s.id, 26))}</text>` +
                `<text class="node-comp" x="${b.x + 14}" y="${b.y + 52}">${esc(clip(comp, 30))}</text></g>`
            );
        })
        .join('');
    const noteShapes = noteBoxes
        .map(
            b =>
                `<rect class="note" x="${b.x}" y="${b.y}" width="${b.w}" height="${b.h}" rx="8"/>` +
                `<text class="note-text" x="${b.x + 12}" y="${b.y + 26}">${esc(clip(scrub((b.note.data?.text ?? '').split('\n')[0]), 32))}</text>`,
        )
        .join('');
    return (
        `<svg class="diagram" viewBox="${minX} ${minY} ${width} ${height}" role="img" aria-label="Pipeline diagram">` +
        links + noteShapes + stepShapes + '</svg>'
    );
}

const table = (head: string[], rows: string[][]) =>
    `<table><tr>${head.map(h => `<th>${h}</th>`).join('')}</tr>` +
    rows.map(r => `<tr>${r.map(c => `<td>${c}</td>`).join('')}</tr>`).join('') +
    '</table>';

export function pipelineDocument(input: DocInput): string {
    const doc = (input.pipeline ?? {}) as { nodes?: Step[]; edges?: Edge[]; notes?: Node<NoteData>[] };
    const nodes = doc.nodes ?? [];
    const edges = doc.edges ?? [];
    const notes = doc.notes ?? [];
    const repo = input.repo;
    const ordered = runOrder(nodes, edges);
    const labelOf = (id: string) => nodes.find(n => n.id === id)?.data.label || id;
    const stepLink = (id: string) => `<a href="#step-${esc(id)}">${esc(labelOf(id))}</a>`;

    const counts = [
        `${nodes.length} step${nodes.length === 1 ? '' : 's'}`,
        `${edges.length} link${edges.length === 1 ? '' : 's'}`,
        ...(notes.length ? [`${notes.length} note${notes.length === 1 ? '' : 's'}`] : []),
    ].join(', ');
    const when = input.generatedAt.toISOString().slice(0, 16).replace('T', ' ') + ' UTC';

    const ends = (kind: string) =>
        ordered
            .filter(s => kindOf(s) === kind)
            .map(s => {
                const subtitle = deriveNodeSubtitle(s.data.componentId, s.data.properties);
                return `<li>${stepLink(s.id)}${subtitle ? ` <span class="muted">${esc(scrub(subtitle))}</span>` : ''}</li>`;
            })
            .join('');
    const overview =
        `<section id="overview"><h2>Overview</h2>` +
        `<h3>Reads from</h3><ul>${ends('source') || '<li class="muted">Nothing</li>'}</ul>` +
        `<h3>Writes to</h3><ul>${ends('sink') || '<li class="muted">Nothing</li>'}</ul></section>`;

    const picture = diagram(nodes, edges, notes);
    const diagramSection = picture ? `<section id="diagram"><h2>Diagram</h2>${picture}</section>` : '';

    const notesSection = notes.length
        ? `<section id="notes"><h2>Notes</h2>${notes
              .map(n => `<blockquote>${esc(scrub(n.data?.text ?? '')).replace(/\n/g, '<br>')}</blockquote>`)
              .join('')}</section>`
        : '';

    const stepSections = ordered
        .map((s, i) => {
            const manifest = getManifest(s.data.componentId);
            const kind = kindOf(s);
            const into = edges.filter(e => e.target === s.id);
            const out = edges.filter(e => e.source === s.id);
            const via = (e: Edge, id: string) => stepLink(id) + (linkType(e) === 'main' ? '' : ` <span class="muted">(${esc(linkType(e))})</span>`);
            const links = [
                into.length ? `Reads from ${into.map(e => via(e, e.source)).join(', ')}.` : '',
                out.length ? `Feeds ${out.map(e => via(e, e.target)).join(', ')}.` : '',
            ].filter(Boolean).join(' ');
            const rows = settings(s, repo);
            const columns = resolveOutputSchema(s.id, nodes, edges);
            return (
                `<section class="step" id="step-${esc(s.id)}">` +
                `<h3>${i + 1}. ${esc(s.data.label || s.id)}</h3>` +
                `<p class="muted">${esc(manifest?.label ?? s.data.componentId ?? 'Unknown component')} · <code>${esc(s.data.componentId ?? '')}</code> · ${esc(kind.charAt(0).toUpperCase() + kind.slice(1))}</p>` +
                (s.data.disabled ? '<p class="off-note">Disabled: skipped when the pipeline runs.</p>' : '') +
                (manifest?.description ? `<p>${esc(manifest.description)}</p>` : '') +
                (links ? `<p>${links}</p>` : '') +
                (rows.length
                    ? `<h4>Settings</h4>${table(['Setting', 'Value'], rows.map(r => [esc(r.label), r.html]))}`
                    : '<p class="muted">No settings.</p>') +
                (columns.length
                    ? `<h4>Columns it produces</h4>${table(['Column', 'Type', 'Nullable'], columns.map(c => [`<code>${esc(c.name)}</code>`, esc(c.type), c.nullable ? 'Yes' : 'No']))}`
                    : '') +
                '</section>'
            );
        })
        .join('');
    const stepsSection = `<section id="steps"><h2>Steps</h2>${stepSections || '<p class="muted">This pipeline has no steps.</p>'}</section>`;

    const users = new Map<string, string[]>();
    for (const s of ordered) for (const id of connectionsOf(s)) users.set(id, [...(users.get(id) ?? []), s.id]);
    const connectionsSection = users.size
        ? `<section id="connections"><h2>Connections</h2>${table(
              ['Connection', 'Kind', 'Used by'],
              [...users].map(([id, stepIds]) => {
                  const conn = repo.find(i => i.id === id && i.type === 'connection');
                  const kind = (conn?.payload as ConnectionPayload | undefined)?.kind ?? '';
                  return [
                      conn ? esc(conn.name) : `<span class="missing">missing connection ${esc(id)}</span>`,
                      esc(kind),
                      stepIds.map(stepLink).join(', '),
                  ];
              }),
          )}<p class="muted">A saved connection's own settings are not included.</p></section>`
        : '';

    const parameters = pipelineRunFields(input.pipeline).parameters ?? {};
    const variables = discoverParams(nodes, {}, Object.keys(parameters));
    const context = repo.find(i => i.id === input.contextId && i.type === 'context');
    const values = (context?.payload as ContextPayload | undefined)?.variables ?? [];
    const mentions = (s: Step, name: string) => {
        const pattern = new RegExp('\\$\\{\\s*' + name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '\\s*\\}');
        return pattern.test(JSON.stringify(s.data.properties ?? {}));
    };
    const variablesSection = variables.length
        ? `<section id="variables"><h2>Context variables</h2><p class="muted">${
              context ? `Values from the context ${esc(context.name)}.` : 'No context was selected, so no values are shown.'
          }</p>${table(
              ['Variable', 'Value', 'Used by'],
              variables.map(name => {
                  const v = values.find(x => x.key === name);
                  const value = !context
                      ? ''
                      : !v
                        ? `<span class="missing">not set in ${esc(context.name)}</span>`
                        : v.secret || isSecretPropKey(name)
                          ? WITHHELD
                          : `<code>${esc(scrub(v.value))}</code>`;
                  return [`<code>${esc(name)}</code>`, value, ordered.filter(s => mentions(s, name)).map(s => stepLink(s.id)).join(', ')];
              }),
          )}</section>`
        : '';

    const parameterRows = Object.entries(parameters);
    const parametersSection = parameterRows.length
        ? `<section id="parameters"><h2>Parameters</h2>${table(
              ['Parameter', 'Type', 'Default', 'Description'],
              parameterRows.map(([name, spec]) => {
                  const secret = spec.type === 'secret' || isSecretPropKey(name);
                  const fallback = isEmpty(spec.default) ? '' : secret ? WITHHELD : `<code>${esc(scrub(String(spec.default)))}</code>`;
                  return [
                      `<code>${esc(name)}</code>${spec.required ? ' <span class="muted">required</span>' : ''}`,
                      esc(spec.type ?? 'string'),
                      fallback,
                      esc(spec.description ?? ''),
                  ];
              }),
          )}</section>`
        : '';

    const contents = [
        ['overview', 'Overview'],
        ...(picture ? [['diagram', 'Diagram']] : []),
        ...(notes.length ? [['notes', 'Notes']] : []),
        ['steps', 'Steps'],
        ...(users.size ? [['connections', 'Connections']] : []),
        ...(variables.length ? [['variables', 'Context variables']] : []),
        ...(parameterRows.length ? [['parameters', 'Parameters']] : []),
    ]
        .map(([id, title]) => `<a href="#${id}">${title}</a>`)
        .join(' · ');

    return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${esc(input.name)} - pipeline documentation</title>
<style>${STYLE}</style>
</head>
<body>
<header><h1>${esc(input.name)}</h1>
<p class="muted">Pipeline documentation generated by Duckle on ${when}. ${counts}.</p>
<nav>${contents}</nav></header>
${overview}${diagramSection}${notesSection}${stepsSection}${connectionsSection}${variablesSection}${parametersSection}
<footer class="muted">Generated from the pipeline file. Values under credential settings, auth headers, passwords inside URLs and connection strings, secret variables and parameters, and saved connections' settings are left out.</footer>
</body>
</html>
`;
}

const STYLE = `
:root { color-scheme: light; --ink: #1d2330; --muted: #5c6577; --line: #d9dee7; --soft: #f4f6f9; --accent: #ed5f22; }
* { box-sizing: border-box; }
body { margin: 0 auto; max-width: 980px; padding: 32px 24px 64px; font: 15px/1.55 system-ui, -apple-system, 'Segoe UI', sans-serif; color: var(--ink); background: #fff; }
h1 { margin: 0 0 4px; font-size: 1.9rem; }
h2 { margin: 40px 0 12px; padding-bottom: 6px; border-bottom: 2px solid var(--line); font-size: 1.3rem; }
h3 { margin: 22px 0 6px; font-size: 1.05rem; }
h4 { margin: 14px 0 6px; font-size: 0.9rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.04em; }
nav a { color: var(--accent); }
a { color: inherit; }
.muted { color: var(--muted); }
code, pre { font-family: ui-monospace, 'Cascadia Code', Consolas, monospace; font-size: 0.85em; }
pre { margin: 0; white-space: pre-wrap; overflow-wrap: anywhere; }
table { width: 100%; border-collapse: collapse; margin: 6px 0 4px; }
th, td { text-align: left; vertical-align: top; padding: 6px 10px; border: 1px solid var(--line); overflow-wrap: anywhere; }
th { background: var(--soft); font-weight: 600; width: 32%; }
table.pairs { margin: 0; }
table.pairs td { border-color: var(--soft); padding: 2px 8px; }
.step { border-top: 1px solid var(--line); padding-top: 4px; }
.withheld { font-style: italic; color: var(--muted); }
.missing, .off-note { color: #a85b00; }
blockquote { margin: 8px 0; padding: 10px 14px; background: #fff8e6; border: 1px solid #f1d58a; border-radius: 8px; }
.diagram { width: 100%; max-height: 520px; background: var(--soft); border: 1px solid var(--line); border-radius: 10px; }
.diagram .node { fill: #fff; stroke: #9aa3b5; stroke-width: 1.5; }
.diagram .node-source { stroke: #2f80ed; }
.diagram .node-sink { stroke: var(--accent); }
.diagram .off { opacity: 0.45; }
.diagram .node-label { font: 600 15px system-ui, sans-serif; fill: var(--ink); }
.diagram .node-comp { font: 12px system-ui, sans-serif; fill: var(--muted); }
.diagram .link { fill: none; stroke: #8a93a6; stroke-width: 2; }
.diagram .link-reject { stroke: #d64545; stroke-dasharray: 6 4; }
.diagram .link:not(.link-main):not(.link-reject) { stroke-dasharray: 4 4; }
.diagram .note { fill: #fff8e6; stroke: #f1d58a; }
.diagram .note-text { font: 12px system-ui, sans-serif; fill: #6b5000; }
footer { margin-top: 48px; padding-top: 12px; border-top: 1px solid var(--line); font-size: 0.85rem; }
@media print { body { padding: 0; } .diagram { max-height: none; } }
`;
