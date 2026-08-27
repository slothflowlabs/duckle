import type { Edge, Node } from '@xyflow/react';
import type { DuckleNodeData } from './pipeline-types';
import type { RepoItem } from './repo-types';
import { getManifest } from './workflow-ui/fields/component-manifests';
import { contextKeyCollisions } from './run-resolve';

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
    'snk.qvd',
    'snk.spatial',
]);

export function validatePipeline(
    nodes: Node<DuckleNodeData>[],
    edges: Edge[],
    repo: RepoItem[] = [],
): ValidationResult {
    if (nodes.length === 0) return EMPTY;

    const issues: ValidationIssue[] = [];
    const push = (i: Omit<ValidationIssue, 'id'>) => {
        issues.push({ id: 'i_' + issues.length, ...i });
    };

    const nodeIds = new Set(nodes.map(n => n.id));

    // ---- SQL name (alias) uniqueness (#102) ----
    // A node alias names its output relation, so two nodes can't share one and
    // an alias can't shadow another node's id (the engine rejects both at
    // compile time; surface it here as an inline error first).
    const aliasOwner = new Map<string, string>();
    for (const node of nodes) {
        const alias = typeof node.data.alias === 'string' ? node.data.alias.trim() : '';
        if (!alias || alias === node.id) continue;
        if (nodeIds.has(alias)) {
            push({
                severity: 'error',
                code: 'alias-collides-with-id',
                message: `${node.data.label}: SQL name '${alias}' is already another node's id. Pick a different name.`,
                nodeId: node.id,
            });
        }
        const prior = aliasOwner.get(alias);
        if (prior) {
            push({
                severity: 'error',
                code: 'duplicate-alias',
                message: `${node.data.label}: SQL name '${alias}' is already used by another node. Each must be unique.`,
                nodeId: node.id,
            });
        } else {
            aliasOwner.set(alias, node.id);
        }
    }

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
        // A saved connection supplies these at run time (merge_generic_connection in
        // duckle-secrets), and the connection WINS over any inline value, so a node
        // carrying a connectionRef is complete even with these blank. Without this
        // the editor reported "'Host' is required" on a pipeline that runs perfectly,
        // and the only way to clear it was to copy the credentials back onto the node,
        // which is the duplication the saved connection exists to remove.
        const CONNECTION_SUPPLIED = new Set([
            'host', 'port', 'database', 'username', 'password',
            'bucket', 'region', 'accessKey', 'secretKey', 'sessionToken',
            'accountName', 'accountKey', 'brokers', 'url', 'endpoint', 'urlStyle',
            'sslmode', 'sslrootcert', 'sslcert', 'sslkey', 'connectTimeout',
            'options', 'connParams',
            'loginUrl', 'clientId', 'clientSecret', 'instanceUrl',
            'accessToken', 'authToken', 'authType', 'authMode', 'account',
        ]);
        const ref = props['connectionRef'];
        const hasConnection = typeof ref === 'string' && ref !== '';
        for (const section of manifest.sections) {
            for (const field of section.fields) {
                if (!field.required) continue;
                if (hasConnection && CONNECTION_SUPPLIED.has(field.key)) continue;
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

        // Inline SQL: the upstream is optional, because SQL that reads its own
        // data is self-contained and the engine runs it happily. What IS a
        // mistake is saying `FROM input` with nothing wired in - the wrapper
        // that defines `input` is only emitted when there is a main upstream,
        // so that fails at run time with "Table with name input does not
        // exist". Checking the SQL rather than the edge is what lets a
        // self-contained node be clean without inventing a fake parent.
        if (node.data.componentId === 'code.sql' || node.data.componentId === 'code.sqltemplate') {
            const sql = typeof props.sql === 'string' ? props.sql : '';
            const readsInput = /\b(?:from|join)\s+"?input"?\b/i.test(sql);
            const hasUpstream = edges.some(e => e.target === node.id);
            if (readsInput && !hasUpstream) {
                push({
                    severity: 'error',
                    code: 'missing-required-input',
                    message: `${node.data.label}: the SQL reads 'input', but nothing is connected to this node.`,
                    nodeId: node.id,
                });
            }
            // Raw and Pure mode both skip the wrapper, so `input` is never
            // defined in them even when an upstream IS connected - reference the
            // node by its own id instead.
            if (readsInput && (props.rawSql === true || props.pureSql === true)) {
                push({
                    severity: 'error',
                    code: 'missing-required-input',
                    message: `${node.data.label}: Raw / Pure SQL mode does not define 'input' - reference the upstream node by its id, e.g. SELECT * FROM "node_id".`,
                    nodeId: node.id,
                });
            }
            // Pure mode produces no relation of its own, so anything reading
            // from this node gets "Table with name <id> does not exist" unless
            // the SQL creates it. That is the failure this issue was reported
            // against, attributed to Raw mode.
            if (props.pureSql === true && edges.some(e => e.source === node.id)) {
                const idRe = node.id.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
                const createsOwn = new RegExp(
                    `\\bcreate\\s+(or\\s+replace\\s+)?(temp\\s+|temporary\\s+)?(table|view)\\s+"?${idRe}"?(?!\\w)`,
                    'i',
                ).test(sql);
                if (!createsOwn) {
                    push({
                        severity: 'error',
                        code: 'pure-sql-no-output',
                        message: `${node.data.label}: Pure SQL mode creates no output relation, but a node reads from this one. Create it in the SQL (CREATE OR REPLACE TABLE "${node.id}" AS ...) or turn Pure SQL off.`,
                        nodeId: node.id,
                    });
                }
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

    // ---- Context key collisions (#204) ----
    // Two contexts defining the same bare key share one slot in
    // buildContextVars' flat map, so `${KEY}` silently resolves to whichever
    // context is last in repo order. Warn (not error) and point at the
    // unambiguous `${context.KEY}` form. A single context never collides.
    for (const c of contextKeyCollisions(repo)) {
        push({
            severity: 'warning',
            code: 'duplicate-context-key',
            message:
                `Variable "${c.key}" is defined by ${c.contexts.length} contexts ` +
                `(${c.contexts.join(', ')}); a bare \${${c.key}} resolves to only one. ` +
                `Use \${context.${c.key}} to pick a specific context.`,
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
