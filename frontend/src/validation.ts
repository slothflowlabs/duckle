import type { Edge, Node } from '@xyflow/react';
import type { DuckleNodeData } from './pipeline-types';
import { getManifest } from './workflow-ui/fields/component-manifests';

export type ValidationIssue = {
    id: string;
    severity: 'error' | 'warning';
    code: string;
    message: string;
    nodeId?: string;
    edgeId?: string;
};

export type ValidationResult = {
    issues: ValidationIssue[];
    errorCount: number;
    warningCount: number;
    errorByNode: Record<string, ValidationIssue[]>;
};

const EMPTY: ValidationResult = {
    issues: [],
    errorCount: 0,
    warningCount: 0,
    errorByNode: {},
};

// Sinks that write to a file / object-store path (so an empty path is a
// real error). Database, warehouse, vector-DB, message-broker and HTTP
// sinks write to a connection / table / topic instead and must NOT be
// required to have a path (issue #8).
const PATH_REQUIRED_SINKS = new Set<string>([
    'snk.csv',
    'snk.tsv',
    'snk.parquet',
    'snk.json',
    'snk.jsonl',
    'snk.excel',
    'snk.xml',
    'snk.yaml',
    'snk.toml',
    'snk.avro',
    'snk.spatial',
]);

export function validatePipeline(
    nodes: Node<DuckleNodeData>[],
    edges: Edge[],
): ValidationResult {
    if (nodes.length === 0) return EMPTY;

    const issues: ValidationIssue[] = [];
    const push = (i: Omit<ValidationIssue, 'id'>) => {
        issues.push({ id: 'i_' + issues.length, ...i });
    };

    const nodeIds = new Set(nodes.map(n => n.id));

    // ---- Per-node checks ----
    for (const node of nodes) {
        if (node.data.disabled) continue;
        const manifest = getManifest(node.data.componentId);
        if (!manifest) {
            push({
                severity: 'warning',
                code: 'unknown-component',
                message: `Unknown component '${node.data.componentId ?? '?'}'.`,
                nodeId: node.id,
            });
            continue;
        }

        // Required fields populated
        const props = node.data.properties ?? {};
        for (const section of manifest.sections) {
            for (const field of section.fields) {
                if (!field.required) continue;
                const v = props[field.key];
                const empty =
                    v === undefined ||
                    v === null ||
                    v === '' ||
                    (Array.isArray(v) && v.length === 0);
                if (empty) {
                    push({
                        severity: 'error',
                        code: 'missing-required-field',
                        message: `${node.data.label}: '${field.label}' is required.`,
                        nodeId: node.id,
                    });
                }
            }
        }

        // Required inputs connected. Inputs without `optional: true`
        // must have at least one upstream edge of any matching type
        // (we accept the edge regardless of connectionType for now -
        // the picker already enforces compatibility on creation).
        const inputs = manifest.ports?.inputs ?? [];
        const required = inputs.filter(p => !p.optional);
        if (required.length > 0) {
            const hasMain = edges.some(e => e.target === node.id);
            if (!hasMain) {
                push({
                    severity: 'error',
                    code: 'missing-required-input',
                    message: `${node.data.label} has no upstream connection.`,
                    nodeId: node.id,
                });
            }
        }

        // Filter sanity - warn only when the predicate is genuinely empty.
        // The visual builder writes `predicate` as an object that always
        // carries a compiled `.sql` string (raw mode carries `rawSql`), and
        // the engine also accepts a top-level `filterSql`. The old check only
        // handled a plain string, so any visually-built predicate (the common
        // case) was wrongly reported as "empty - every row will pass" even
        // though it filtered correctly. This now mirrors the engine's
        // filter_predicate_sql + filterSql fallback exactly.
        if (node.data.componentId === 'xf.filter') {
            const raw = props.predicate;
            let pred = '';
            if (typeof raw === 'string') {
                pred = raw.trim();
            } else if (raw && typeof raw === 'object') {
                const o = raw as { sql?: unknown; rawSql?: unknown; mode?: unknown };
                if (typeof o.sql === 'string' && o.sql.trim()) {
                    pred = o.sql.trim();
                } else if (o.mode === 'raw' && typeof o.rawSql === 'string') {
                    pred = o.rawSql.trim();
                }
            }
            if (!pred && typeof props.filterSql === 'string') {
                pred = props.filterSql.trim();
            }
            if (!pred) {
                push({
                    severity: 'warning',
                    code: 'empty-filter-predicate',
                    message: `${node.data.label}: predicate is empty - every row will pass.`,
                    nodeId: node.id,
                });
            }
        }

        // Only FILE / object-store sinks need an output path. Database,
        // warehouse, message-broker and HTTP sinks (snk.oracle,
        // snk.sqlserver, snk.postgres, snk.mongodb, snk.kafka, ...) write
        // to a connection / table / topic and have no path - requiring one
        // wrongly blocked loading data into them (issue #8). Per-connector
        // required fields are validated from the component manifest
        // elsewhere; this check is just for the file-path formats.
        if (PATH_REQUIRED_SINKS.has(node.data.componentId ?? '')) {
            const path =
                typeof props.path === 'string' ? props.path.trim() : '';
            if (!path) {
                push({
                    severity: 'error',
                    code: 'sink-without-path',
                    message: `${node.data.label}: output path is required.`,
                    nodeId: node.id,
                });
            }
        }
    }

    // ---- Edge checks ----
    for (const e of edges) {
        if (!nodeIds.has(e.source) || !nodeIds.has(e.target)) {
            push({
                severity: 'warning',
                code: 'dangling-edge',
                message: `Edge ${e.id} references a missing node.`,
                edgeId: e.id,
            });
        }
    }

    // ---- Cycle detection on data-flow edges ----
    if (hasCycle(nodes, edges)) {
        push({
            severity: 'error',
            code: 'cycle',
            message: 'Pipeline contains a cycle in the data-flow graph.',
        });
    }

    // ---- Bucket by node id for inline UI ----
    const errorByNode: Record<string, ValidationIssue[]> = {};
    let errorCount = 0;
    let warningCount = 0;
    for (const i of issues) {
        if (i.severity === 'error') errorCount += 1;
        else warningCount += 1;
        if (i.nodeId) {
            (errorByNode[i.nodeId] ??= []).push(i);
        }
    }

    return { issues, errorCount, warningCount, errorByNode };
}

function hasCycle(
    nodes: Node<DuckleNodeData>[],
    edges: Edge[],
): boolean {
    const adj = new Map<string, string[]>();
    const inDegree = new Map<string, number>();
    for (const n of nodes) {
        adj.set(n.id, []);
        inDegree.set(n.id, 0);
    }
    const dataEdges = edges.filter(e => {
        const t = (e.data as { connectionType?: string } | undefined)?.connectionType;
        return !t || t === 'main' || t === 'lookup' || t === 'reject' || t === 'filter';
    });
    for (const e of dataEdges) {
        if (!adj.has(e.source) || !adj.has(e.target)) continue;
        adj.get(e.source)!.push(e.target);
        inDegree.set(e.target, (inDegree.get(e.target) ?? 0) + 1);
    }
    const queue: string[] = [];
    for (const [id, d] of inDegree) if (d === 0) queue.push(id);
    let processed = 0;
    while (queue.length > 0) {
        const id = queue.shift()!;
        processed += 1;
        for (const child of adj.get(id) ?? []) {
            const d = (inDegree.get(child) ?? 0) - 1;
            inDegree.set(child, d);
            if (d === 0) queue.push(child);
        }
    }
    return processed !== nodes.length;
}
