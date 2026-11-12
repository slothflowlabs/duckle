// What a link carried in a run, and the breakpoint a link can carry.

import type { Edge } from '@xyflow/react';
import type { NodePreview, RunResult } from './tauri-bridge';

/** The port a link leaves its step from, as the engine names it. */
export function linkPort(edge: Pick<Edge, 'sourceHandle'>): string {
    return edge.sourceHandle || 'main';
}

/**
 * The rows a link carried in a run: its source step's own preview for a link
 * from `main`, the rows its port sent otherwise. Null when the run holds none,
 * because the step did not run or the run kept no previews.
 */
export function linkRows(result: RunResult | null, edge: Pick<Edge, 'source' | 'sourceHandle'>): NodePreview | null {
    if (!result) return null;
    const port = linkPort(edge);
    if (port === 'main') return result.preview.find(p => p.node_id === edge.source) ?? null;
    return result.link_previews?.find(l => l.node_id === edge.source && l.port === port) ?? null;
}

/**
 * The links with a breakpoint set on `edgeId`, or taken off when `condition` is
 * blank. Everything else on the link (its type, label, condition) is kept.
 */
export function withBreakpoint(edges: Edge[], edgeId: string, condition: string): Edge[] {
    const wanted = condition.trim();
    return edges.map(e => {
        if (e.id !== edgeId) return e;
        const rest = { ...((e.data ?? {}) as Record<string, unknown>) };
        delete rest.breakpoint;
        return { ...e, data: wanted ? { ...rest, breakpoint: wanted } : rest };
    });
}
