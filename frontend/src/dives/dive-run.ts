// The "never-stale" read: run a dive's SQL against DuckDB and return columns +
// rows. v1 synthesizes a one-node code.sql pipeline and runs it through the
// existing engine path (SSE progress / cancel / ${workspace} resolution come
// for free, and it is dual-backend desktop + web). See docs/design/dives.md.

import type { Node } from '@xyflow/react';
import type { Column, DuckleNodeData } from '../pipeline-types';
import { runPipeline } from '../tauri-bridge';
import type { Dive, DiveParam } from './dive-types';

export interface DiveResult {
    columns: Column[];
    rows: Record<string, unknown>[];
    /** Each column's DuckDB type, aligned with `columns`; a geometry's CRS is written there (#372). */
    sqlTypes?: string[];
}

/** A param value coerced to a SAFE typed SQL literal. Strings/dates are
 *  single-quote escaped; numbers are validated; bools become TRUE/FALSE. Never
 *  free-text concatenation - this is the only place user/AI values enter SQL. */
function safeLiteral(p: DiveParam, raw: unknown): string {
    const v = raw ?? p.default;
    switch (p.type) {
        case 'number': {
            const n = Number(v);
            if (!Number.isFinite(n)) throw new Error(`Dive param "${p.name}" is not a number.`);
            return String(n);
        }
        case 'bool':
            return v === true || v === 'true' ? 'TRUE' : 'FALSE';
        case 'date':
        case 'string':
        default:
            return `'${String(v ?? '').replace(/'/g, "''")}'`;
    }
}

/** Replace `:name` tokens in the SQL with safe typed literals from the declared
 *  param set. Only declared params are substituted. */
export function resolveParams(
    sql: string,
    params: DiveParam[] | undefined,
    values: Record<string, unknown> = {},
): string {
    if (!params || params.length === 0) return sql;
    let out = sql;
    for (const p of params) {
        const lit = safeLiteral(p, values[p.name]);
        out = out.replace(new RegExp(`:${p.name}\\b`, 'g'), lit);
    }
    return out;
}

/**
 * Run a dive and return its current columns + rows. Called on every open, which
 * is what makes a dive never-stale. v1 note: the SQL must be self-contained
 * (read its source via read_parquet/read_csv_auto or an attached table); the
 * AI-generation flow (Phase 2) emits SQL in that shape.
 */
export async function runDive(
    dive: Dive,
    workspacePath?: string | null,
    paramValues?: Record<string, unknown>,
): Promise<DiveResult> {
    const sql = resolveParams(
        dive.query.sql,
        dive.query.params,
        paramValues ?? dive.state?.paramValues ?? {},
    );
    const node: Node<DuckleNodeData> = {
        id: 'dive_sql',
        type: 'duckle',
        position: { x: 0, y: 0 },
        data: { label: dive.title || 'Dive', componentId: 'code.sql', properties: { sql } },
    };
    const result = await runPipeline([node], [], undefined, dive.id, workspacePath ?? null, dive.title);
    if (!result) throw new Error('No backend available to run the dive.');
    if (result.status === 'error') throw new Error(result.error || 'Dive query failed.');
    const preview =
        result.preview.find((p) => p.node_id === 'dive_sql') ??
        result.preview[result.preview.length - 1];
    return { columns: preview?.columns ?? [], rows: preview?.rows ?? [], sqlTypes: preview?.sql_types };
}
