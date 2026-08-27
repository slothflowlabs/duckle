import type { Node } from '@xyflow/react';
import type { DuckleNodeData } from './pipeline-types';
import type { ContextPayload, RepoItem, RoutinePayload } from './repo-types';

/**
 * Resolve a pipeline's nodes for execution:
 *   1. Inline a referenced SQL routine into Custom-SQL nodes.
 *   2. Substitute `${var}` / `${context.var}` references in field values
 *      with the workspace's context variables.
 *   3. Map a child-pipeline reference (Run Job / Iterate / Foreach / Try)
 *      stored as a workspace pipeline id to its on-disk file path, which
 *      is what the engine reads.
 *
 * Run on the working nodes right before they're sent to the engine, so
 * the canvas keeps the un-substituted, editable values.
 */

// Props that hold a reference to another pipeline the engine will read
// from disk. The dropdown stores a portable pipeline id; the engine needs
// a file path, so we resolve here at run time.
const PIPELINE_REF_KEYS = [
    'pipelineRef',
    'iteratePipelineRef',
    'foreachPipelineRef',
    'fallbackPipelineRef',
];

function joinPath(dir: string, ...parts: string[]): string {
    const sep = dir.includes('\\') && !dir.includes('/') ? '\\' : '/';
    return [dir.replace(/[/\\]+$/, ''), ...parts].join(sep);
}

/**
 * Contexts in the order they should be merged: lowest layer first, and repo
 * order within a layer (#204).
 *
 * `sort` is required to be stable by the language, so contexts sharing a
 * priority keep the order the repo lists them in - which is what the flat
 * merge did before priorities existed, so a workspace that sets none behaves
 * exactly as it used to.
 */
function contextsByLayer(repo: RepoItem[]): { item: RepoItem; payload: ContextPayload }[] {
    return repo
        .filter(item => item.type === 'context')
        .map(item => ({ item, payload: item.payload as ContextPayload | undefined }))
        .filter((c): c is { item: RepoItem; payload: ContextPayload } => !!c.payload?.variables)
        .sort((a, b) => (a.payload.priority ?? 0) - (b.payload.priority ?? 0));
}

export function buildContextVars(repo: RepoItem[]): Record<string, string> {
    const out: Record<string, string> = {};
    // Ascending layer, so a higher-priority context lands last and wins.
    for (const { item, payload } of contextsByLayer(repo)) {
        for (const v of payload.variables) {
            // Both the bare key and a context-namespaced key resolve.
            out[v.key] = v.value;
            out[`${item.name}.${v.key}`] = v.value;
        }
    }
    return out;
}

/**
 * Bare context-variable keys that are genuinely ambiguous (#204).
 *
 * Two contexts defining the same key only compete when they sit on the SAME
 * layer, because nothing then says which should win and `${KEY}` resolves to
 * whichever the repo happens to list last. A key defined on different layers
 * is a declared override - a base plus an environment on top - and reporting
 * that was the complaint: every intended override looked like a mistake and
 * had to be resolved by hand.
 *
 * Returns one entry per ambiguous key with the contexts that define it, so the
 * validator can warn. A single context can never collide, so existing
 * single-context workspaces never trigger this.
 */
export function contextKeyCollisions(repo: RepoItem[]): { key: string; contexts: string[] }[] {
    // key -> layer -> contexts defining it on that layer
    const byKey = new Map<string, Map<number, string[]>>();
    for (const item of repo) {
        if (item.type !== 'context') continue;
        const payload = item.payload as ContextPayload | undefined;
        if (!payload?.variables) continue;
        const layer = payload.priority ?? 0;
        // A key repeated inside ONE context is not a cross-context collision.
        const seen = new Set<string>();
        for (const v of payload.variables) {
            if (seen.has(v.key)) continue;
            seen.add(v.key);
            let layers = byKey.get(v.key);
            if (!layers) {
                layers = new Map();
                byKey.set(v.key, layers);
            }
            const list = layers.get(layer);
            if (list) list.push(item.name);
            else layers.set(layer, [item.name]);
        }
    }
    const out: { key: string; contexts: string[] }[] = [];
    for (const [key, layers] of byKey) {
        for (const contexts of layers.values()) {
            if (contexts.length > 1) out.push({ key, contexts });
        }
    }
    return out;
}

function pad(n: number): string {
    return String(n).padStart(2, '0');
}

/**
 * Format a date object according to the given builtin base name.
 */
function formatTimeBuiltin(base: string, d: Date): string {
    const ymd = `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}`;
    const hms = `${pad(d.getUTCHours())}${pad(d.getUTCMinutes())}${pad(d.getUTCSeconds())}`;
    switch (base) {
        case 'date':
            return ymd;
        case 'time':
            return hms;
        case 'datetime':
            return `${ymd}_${hms}`;
        case 'timestamp':
            return String(Math.floor(d.getTime() / 1000));
        case 'now':
            return `${ymd}T${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}Z`;
        default:
            return '';
    }
}

/**
 * Parse a relative offset like `+1d`, `-2h`, `+30m`, `-45s` or a combination
 * (`+1d6h30m`) into milliseconds (issue #191), mirroring Rust context.rs.
 */
function parseOffset(s: string): number | null {
    const sign = s.startsWith('-') ? -1 : 1;
    const rest = s.startsWith('+') || s.startsWith('-') ? s.slice(1) : s;
    if (!rest) return null;

    let totalMs = 0;
    let numStr = '';
    for (let i = 0; i < rest.length; i++) {
        const ch = rest[i];
        if (ch >= '0' && ch <= '9') {
            numStr += ch;
            continue;
        }
        if (!numStr) return null;
        const n = parseInt(numStr, 10);
        numStr = '';
        switch (ch) {
            case 'd':
                totalMs += n * 24 * 60 * 60 * 1000;
                break;
            case 'h':
                totalMs += n * 60 * 60 * 1000;
                break;
            case 'm':
                totalMs += n * 60 * 1000;
                break;
            case 's':
                totalMs += n * 1000;
                break;
            default:
                return null;
        }
    }
    if (numStr) return null;
    return totalMs * sign;
}

/**
 * Resolve a time-builtin placeholder name, with an optional relative offset,
 * to its formatted value (issue #191).
 */
export function resolveTimeBuiltin(name: string, now: Date = new Date()): string | null {
    const bases = ['timestamp', 'datetime', 'date', 'time', 'now'];
    for (const base of bases) {
        if (name === base) {
            return formatTimeBuiltin(base, now);
        }
        if (name.startsWith(base)) {
            const rest = name.slice(base.length);
            if (rest.startsWith('+') || rest.startsWith('-')) {
                const offsetMs = parseOffset(rest);
                if (offsetMs !== null) {
                    const shifted = new Date(now.getTime() + offsetMs);
                    return formatTimeBuiltin(base, shifted);
                }
            }
        }
    }
    return null;
}

/**
 * Dynamic date/time placeholders for timestamped source / sink paths, e.g.
 * `${workspace}/exports/${date}/orders.parquet` or `out_${datetime}.csv`.
 * All UTC so a run produces the same names on any machine / in CI, and
 * mirrors the engine's insert_time_builtins (context.rs):
 *   ${date}      -> YYYY-MM-DD
 *   ${time}      -> HHMMSS
 *   ${datetime}  -> YYYY-MM-DD_HHMMSS   (filename-safe, no colons)
 *   ${timestamp} -> epoch seconds
 *   ${now}       -> ISO-8601 (has colons; for values, not paths)
 */
function timeBuiltins(now: Date = new Date()): Record<string, string> {
    return {
        date: formatTimeBuiltin('date', now),
        time: formatTimeBuiltin('time', now),
        datetime: formatTimeBuiltin('datetime', now),
        timestamp: formatTimeBuiltin('timestamp', now),
        now: formatTimeBuiltin('now', now),
    };
}

/**
 * Built-in placeholders available everywhere without defining a context.
 * `${workspace}` (and the `${projectroot}` alias) resolve to the active
 * workspace root, so paths can be written relative to it and the whole
 * workspace folder stays portable when it is copied or moved (#37). Path
 * separators are normalized to `/` (DuckDB accepts them on every platform).
 * The date/time builtins are always present, even without a workspace.
 */
export function builtinVars(workspacePath?: string | null, now: Date = new Date()): Record<string, string> {
    const vars = timeBuiltins(now);
    if (workspacePath) {
        const root = workspacePath.replace(/\\/g, '/');
        vars.workspace = root;
        vars.projectroot = root;
    }
    return vars;
}

function substituteString(value: string, vars: Record<string, string>, now: Date = new Date()): string {
    return value.replace(/\$\{([^}]+)\}/g, (match, expr) => {
        const key = String(expr).trim();
        if (Object.prototype.hasOwnProperty.call(vars, key)) {
            return vars[key]!;
        }
        const timeVal = resolveTimeBuiltin(key, now);
        if (timeVal !== null) {
            return timeVal;
        }
        return match;
    });
}

export function substituteDeep(value: unknown, vars: Record<string, string>, now: Date = new Date()): unknown {
    if (typeof value === 'string') return substituteString(value, vars, now);
    if (Array.isArray(value)) return value.map(v => substituteDeep(v, vars, now));
    if (value && typeof value === 'object') {
        const out: Record<string, unknown> = {};
        for (const [k, v] of Object.entries(value)) out[k] = substituteDeep(v, vars, now);
        return out;
    }
    return value;
}

// Builtins that resolve without a user-supplied value, mirrored from the
// engine's discover_parameters (context.rs). A `${name}` referencing one of
// these (or an ${ENV:KEY} secret) is never treated as a run parameter.
const PARAM_BUILTINS = new Set([
    'date',
    'time',
    'datetime',
    'timestamp',
    'now',
    'workspace',
    'projectroot',
]);

/**
 * Discover the `${name}` placeholders a pipeline's nodes reference that are NOT
 * already resolvable - i.e. not a date/time or ${workspace} builtin, not an
 * ${ENV:KEY} secret, and not provided by `knownVars` (the active contexts).
 * These are the values the editor can prompt for at run time (issue #127), so a
 * pipeline whose placeholders are all context-backed runs without a prompt.
 */
export function discoverParams(
    nodes: Node<DuckleNodeData>[],
    knownVars: Record<string, string>,
): string[] {
    const found = new Set<string>();
    const scan = (value: unknown): void => {
        if (typeof value === 'string') {
            for (const m of value.matchAll(/\$\{([^}]+)\}/g)) {
                const key = String(m[1]).trim();
                if (!key || key.startsWith('ENV:')) continue;
                if (PARAM_BUILTINS.has(key) || resolveTimeBuiltin(key) !== null) continue;
                if (Object.prototype.hasOwnProperty.call(knownVars, key)) continue;
                found.add(key);
            }
        } else if (Array.isArray(value)) {
            value.forEach(scan);
        } else if (value && typeof value === 'object') {
            Object.values(value).forEach(scan);
        }
    };
    for (const node of nodes) scan(node.data.properties ?? {});
    return Array.from(found).sort();
}

export function resolveForRun(
    nodes: Node<DuckleNodeData>[],
    repo: RepoItem[],
    workspacePath?: string | null,
    extraVars?: Record<string, string>,
    runtimeParams?: Record<string, string>,
): Node<DuckleNodeData>[] {
    // One `now` for the whole pass so every placeholder (and every offset) in a
    // run stamps the exact same instant (mirrors context.rs:198-200).
    const now = new Date();
    // Built-in workspace placeholders first, so an explicit context variable of
    // the same name (unusual) still wins. Global-context (extraVars) is merged
    // next so its runtime values override the static context defaults, then the
    // run-time input parameters (issue #127) win over everything.
    const vars = {
        ...builtinVars(workspacePath, now),
        ...buildContextVars(repo),
        ...(extraVars ?? {}),
        ...(runtimeParams ?? {}),
    };
    const sqlRoutines = new Map<string, string>();
    // Map a workspace pipeline id (or name) to its on-disk file path so a
    // dropdown-stored id resolves to something the engine can read.
    const pipelinePaths = new Map<string, string>();
    for (const item of repo) {
        if (item.type === 'routine') {
            const payload = item.payload as RoutinePayload | undefined;
            if (payload?.language === 'sql' && payload.code) {
                sqlRoutines.set(item.id, payload.code);
                sqlRoutines.set(item.name, payload.code);
            }
        } else if (item.type === 'pipeline' && workspacePath) {
            const file = joinPath(workspacePath, 'pipelines', `${item.id}.json`);
            pipelinePaths.set(item.id, file);
            pipelinePaths.set(item.name, file);
        }
    }
    const hasVars = Object.keys(vars).length > 0;

    return nodes.map(node => {
        const props = { ...(node.data.properties ?? {}) } as Record<string, unknown>;

        // Inline a referenced SQL routine when there's no inline SQL.
        if (node.data.componentId === 'code.sql' || node.data.componentId === 'code.sqltemplate') {
            const ref = typeof props.routineRef === 'string' ? props.routineRef : '';
            const inline = typeof props.sql === 'string' ? props.sql.trim() : '';
            if (ref && !inline && sqlRoutines.has(ref)) {
                props.sql = sqlRoutines.get(ref);
            }
        }

        const resolved = hasVars
            ? (substituteDeep(props, vars, now) as Record<string, unknown>)
            : props;

        // Resolve child-pipeline ids to file paths. A value that isn't a
        // known pipeline id/name (a hand-typed literal path from before the
        // picker existed) is left untouched.
        if (pipelinePaths.size > 0) {
            for (const key of PIPELINE_REF_KEYS) {
                const v = resolved[key];
                if (typeof v === 'string' && pipelinePaths.has(v)) {
                    resolved[key] = pipelinePaths.get(v);
                }
            }
        }

        return { ...node, data: { ...node.data, properties: resolved } };
    });
}
