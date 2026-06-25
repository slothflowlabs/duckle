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

export function buildContextVars(repo: RepoItem[]): Record<string, string> {
    const out: Record<string, string> = {};
    for (const item of repo) {
        if (item.type !== 'context') continue;
        const payload = item.payload as ContextPayload | undefined;
        if (!payload?.variables) continue;
        for (const v of payload.variables) {
            // Both the bare key and a context-namespaced key resolve.
            out[v.key] = v.value;
            out[`${item.name}.${v.key}`] = v.value;
        }
    }
    return out;
}

function pad(n: number): string {
    return String(n).padStart(2, '0');
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
function timeBuiltins(): Record<string, string> {
    const d = new Date();
    const ymd = `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}`;
    const hms = `${pad(d.getUTCHours())}${pad(d.getUTCMinutes())}${pad(d.getUTCSeconds())}`;
    return {
        date: ymd,
        time: hms,
        datetime: `${ymd}_${hms}`,
        timestamp: String(Math.floor(d.getTime() / 1000)),
        now: `${ymd}T${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}Z`,
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
export function builtinVars(workspacePath?: string | null): Record<string, string> {
    const vars = timeBuiltins();
    if (workspacePath) {
        const root = workspacePath.replace(/\\/g, '/');
        vars.workspace = root;
        vars.projectroot = root;
    }
    return vars;
}

function substituteString(value: string, vars: Record<string, string>): string {
    return value.replace(/\$\{([^}]+)\}/g, (match, expr) => {
        const key = String(expr).trim();
        return Object.prototype.hasOwnProperty.call(vars, key) ? vars[key]! : match;
    });
}

export function substituteDeep(value: unknown, vars: Record<string, string>): unknown {
    if (typeof value === 'string') return substituteString(value, vars);
    if (Array.isArray(value)) return value.map(v => substituteDeep(v, vars));
    if (value && typeof value === 'object') {
        const out: Record<string, unknown> = {};
        for (const [k, v] of Object.entries(value)) out[k] = substituteDeep(v, vars);
        return out;
    }
    return value;
}

export function resolveForRun(
    nodes: Node<DuckleNodeData>[],
    repo: RepoItem[],
    workspacePath?: string | null,
    extraVars?: Record<string, string>,
): Node<DuckleNodeData>[] {
    // Built-in workspace placeholders first, so an explicit context variable of
    // the same name (unusual) still wins. Global-context (extraVars) is merged
    // last so its runtime values override the static context defaults.
    const vars = { ...builtinVars(workspacePath), ...buildContextVars(repo), ...(extraVars ?? {}) };
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
            ? (substituteDeep(props, vars) as Record<string, unknown>)
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
