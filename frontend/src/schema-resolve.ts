import type { NodeAnalysis, SqlDiagnostic } from './tauri-bridge';
import type { Edge, Node } from '@xyflow/react';
import type { Column, DataType, DuckleNodeData } from './pipeline-types';
import { getManifest } from './workflow-ui/fields/component-manifests';
import type { Aggregation, GeneratorColumn } from './workflow-ui/fields/types';

type KvPair = { key: string; value: string };

const NUMERIC: DataType[] = ['int32', 'int64', 'float32', 'float64', 'decimal'];

/** What a Data Generator column comes out as. Everything unlisted is text. */
function generatedType(generator: string): DataType {
    const types: Record<string, DataType> = {
        sequence: 'int64',
        integer: 'int64',
        decimal: 'decimal',
        boolean: 'bool',
        date: 'date',
        timestamp: 'timestamp',
        'address.latitude': 'float64',
        'address.longitude': 'float64',
    };
    return types[generator] ?? 'string';
}

function aggOutputType(func: string, sourceCol: Column | undefined): DataType {
    if (func === 'count' || func === 'count_distinct') return 'int64';
    if (func === 'avg') return 'float64';
    if (func === 'array_agg') return 'json';
    if (func === 'sum') {
        const t = sourceCol?.type;
        if (t && NUMERIC.includes(t)) return t;
        return 'float64';
    }
    return sourceCol?.type ?? 'string';
}

/**
 * #226: schemas DuckDB worked out, for nodes this file does not model.
 *
 * The per-component rules below cover the common transforms exactly, and
 * everything else falls back to a guess: keep the upstream columns and, if the
 * node names an `outputColumn`, append it as text. That guess is wrong in three
 * ways it cannot detect - a transform that adds SEVERAL columns, one that
 * REMOVES a column (Text to Columns with dropSource), and any added column that
 * is not text (`xf.length` produces a BIGINT).
 *
 * Rather than add a rule per component for ever, the engine can be asked what a
 * node really produces: it runs that node's own compiled SQL against a zero-row
 * typed stub of its inputs and reports what came out. That reads nothing - no
 * file, no credential, no network - so it is cheap enough for an editor.
 *
 * It is asynchronous, and this resolver is called synchronously from four
 * places during render. So the answer is CACHED here: the resolver returns its
 * best synchronous guess immediately, and uses the engine's answer on the next
 * render once `deriveSchemaFromEngine` has filled it in.
 */
const derived = new Map<string, Column[]>();

/**
 * #314: what DuckDB objected to, per node.
 *
 * Kept beside the columns rather than thrown away. The bind has always caught a
 * typo; the message, its position and the column DuckDB suggests instead were
 * being discarded, so the editor could say only that the node did not resolve -
 * which is the least useful true thing it could say.
 */
const problems = new Map<string, SqlDiagnostic[]>();

export function diagnosticsFor(nodeId: string): SqlDiagnostic[] {
    return problems.get(nodeId) ?? [];
}

/** What makes one derivation different from another. */
function derivedKey(nodeId: string, componentId: string, props: unknown, upstream: Column[]): string {
    return JSON.stringify([
        nodeId,
        componentId,
        props,
        upstream.map(c => [c.name, c.type]),
    ]);
}

/**
 * Ask the engine what a node produces and remember it.
 *
 * Returns true when the cache changed, so a caller can re-render. Failures are
 * swallowed on purpose: this is an improvement on a guess that already exists,
 * and an editor must not show an error because a schema could not be refined.
 */
export async function deriveSchemaFromEngine(
    nodeId: string,
    nodes: Node<DuckleNodeData>[],
    edges: Edge[],
    analyze: (
        nodes: Node<DuckleNodeData>[],
        edges: Edge[],
        nodeId: string,
        inputs: Array<[string, Column[]]>,
    ) => Promise<NodeAnalysis | null>,
): Promise<boolean> {
    const node = nodes.find(n => n.id === nodeId);
    const componentId = node?.data.componentId;
    if (!node || !componentId) return false;

    const inputs = edges
        .filter(e => e.target === nodeId)
        .map(e => [e.source, edgeSchema(e, nodes, edges, new Set())] as const)
        .filter(([, cols]) => cols.length > 0);
    // A remote source has no upstream by definition, and is exactly the node
    // whose SQL Duckle cannot bind - so it is also the node the dialect-neutral
    // hints (#314) are for. The condition mirrors the engine's own `remote`
    // test (`src.` + authored SQL), so lifting the guard here cannot start
    // showing bind errors for an unconnected transform: the engine returns
    // early for these and never runs anything.
    const props = node.data.properties as Record<string, unknown> | undefined;
    const authoredSql = ['sql', 'query'].some(
        k => typeof props?.[k] === 'string' && (props[k] as string).trim() !== '',
    );
    const remoteSource = componentId.startsWith('src.') && authoredSql;
    if (inputs.length === 0 && !remoteSource) return false;

    const key = derivedKey(nodeId, componentId, node.data.properties, inputs.flatMap(([, c]) => c));
    if (derived.has(key)) return false;

    try {
        const analysis = await analyze(
            nodes,
            edges,
            nodeId,
            inputs.map(([id, cols]) => [id, cols] as [string, Column[]]),
        );
        if (!analysis) return false;

        const found = analysis.diagnostics ?? [];
        const had = problems.get(nodeId) ?? [];
        // Replaced rather than merged, and CLEARED when the node now binds: a
        // stale error under a line the author has already fixed is worse than
        // no error at all.
        if (found.length > 0) problems.set(nodeId, found);
        else problems.delete(nodeId);
        const changed = found.length !== had.length
            || found.some((d, i) => d.message !== had[i]?.message);

        const cols = analysis.columns ?? [];
        if (cols.length === 0) return changed;
        derived.set(key, cols);
        return true;
    } catch {
        // A node that cannot be analysed at all - an unfinished configuration,
        // no engine - keeps the guess and reports nothing. Nothing is worse
        // than before, which is the bar for a background refinement.
        return false;
    }
}

/**
 * Resolve the effective output schema of a node by walking the DAG.
 *
 * - `declared` / `autodetect`: use node.data.schema as-is
 * - computed transforms (project, dropcol, rename, cast, addcol, reorder,
 *   groupby, joins): derive from upstream + properties
 * - everything else with `upstream`: pass the merged upstream schema through
 */
export function resolveOutputSchema(
    nodeId: string,
    nodes: Node<DuckleNodeData>[],
    edges: Edge[],
    visiting: Set<string> = new Set(),
): Column[] {
    if (visiting.has(nodeId)) return [];
    visiting.add(nodeId);
    try {
        const node = nodes.find(n => n.id === nodeId);
        if (!node) return [];
        return computeNodeSchema(node, nodes, edges, visiting);
    } finally {
        visiting.delete(nodeId);
    }
}

/**
 * The schema a node keeps after a run that returned `ran`.
 *
 * A source's schema is the user's declared input schema (set via Autodetect or
 * the Schema panel) and the engine consumes it (e.g. CSV `types=`). A run must
 * NOT overwrite it, or re-running keeps replacing a curated schema (issue #18).
 * Everything else, and a source with no schema yet, takes the run's columns.
 *
 * #364: so does a source whose schema shares no column with what ran. The first
 * run fills an empty schema and later runs keep it, so one taken from an earlier
 * query or driver stayed after the node moved on (an ADBC node first run against
 * DuckDB kept `ID int32` against ClickHouse), and the columns it offered
 * downstream were not there. A schema naming any column the run returned is the
 * user's, and stays.
 */
export function schemaAfterRun(componentId: string | undefined, declared: Column[] | undefined, ran: Column[]): Column[] {
    const isSource = (componentId ?? '').startsWith('src.');
    const ranNames = new Set(ran.map(c => c.name.toLowerCase()));
    const describesRun = (declared ?? []).some(c => ranNames.has(c.name.toLowerCase()));
    const keep = isSource && Array.isArray(declared) && declared.length > 0 && describesRun;
    return keep ? declared! : ran;
}

/**
 * The columns a node's Preview tab heads its rows with.
 *
 * #207 / #206: a run stores this node's OWN preview rows keyed by the engine's
 * OUTPUT columns, so the headers must come from outputSchema, not the raw
 * upstream input names - otherwise headers and row keys mismatch and every cell
 * renders empty (rename shows the old names; geo.create hides geom). Pre-run
 * there are only upstream sample rows (input-keyed), so keep upstreamSchema in
 * that case.
 *
 * #364: the same holds for a node with a schema of its own. Its run rows are
 * keyed by the columns the run returned, which a declared schema need not match,
 * so those rows are headed with the run's columns.
 */
export function previewColumns(o: {
    schemaSource: string | undefined;
    hasOwnRows: boolean;
    ran: Column[] | undefined;
    output: Column[];
    declared: Column[];
    upstream: Column[];
}): Column[] {
    if (o.schemaSource === 'upstream') return o.hasOwnRows ? o.output : o.upstream;
    if (o.hasOwnRows && o.ran && o.ran.length > 0) return o.ran;
    return o.declared.length > 0 ? o.declared : o.upstream;
}

function computeNodeSchema(
    node: Node<DuckleNodeData>,
    nodes: Node<DuckleNodeData>[],
    edges: Edge[],
    visiting: Set<string>,
): Column[] {
    const manifest = getManifest(node.data.componentId);
    const props = node.data.properties ?? {};
    const id = node.data.componentId;

    const upstream = () => mergedUpstream(node.id, nodes, edges, visiting);

    // Declared / autodetect - node owns its schema explicitly. A declared node
    // with no schema yet falls through to what the component computes: many
    // transforms (Group By, joins, Add Column, the AI nodes) are 'declared' so a
    // user may write one, and returning the input here meant their own column
    // logic below never ran.
    if (manifest?.schemaSource === 'declared' && node.data.schema) {
        return node.data.schema;
    }
    if (manifest?.schemaSource === 'autodetect') {
        return node.data.schema ?? [];
    }

    if (id === 'src.generate') {
        return ((props.columns as GeneratorColumn[] | undefined) ?? [])
            .filter(c => c.name)
            .map(c => ({ name: c.name, type: generatedType(c.generator), nullable: Boolean(Number(c.nullPercent)) }));
    }

    // ---- Computed transforms ---------------------------------------------

    if (id === 'xf.project') {
        const selected = (props.columns as string[] | undefined) ?? [];
        const up = upstream();
        if (selected.length === 0) return up;
        const order = new Map(selected.map((n, i) => [n, i]));
        const filtered = up.filter(c => order.has(c.name));
        return filtered.sort((a, b) => (order.get(a.name) ?? 0) - (order.get(b.name) ?? 0));
    }

    if (id === 'xf.dropcol') {
        const dropped = (props.columns as string[] | undefined) ?? [];
        const up = upstream();
        if (dropped.length === 0) return up;
        const set = new Set(dropped);
        return up.filter(c => !set.has(c.name));
    }

    if (id === 'xf.rename') {
        const mapping = (props.mapping as KvPair[] | undefined) ?? [];
        const up = upstream();
        if (mapping.length === 0) return up;
        const m = new Map(mapping.filter(p => p.key && p.value).map(p => [p.key, p.value]));
        return up.map(c => ({ ...c, name: m.get(c.name) ?? c.name }));
    }

    if (id === 'xf.cast') {
        const up = upstream();
        // #144: multi-column cast. Re-type every column listed in `casts`.
        const casts = (props.casts ?? props.columns) as
            | Array<{ column?: string; targetType?: string; type?: string }>
            | undefined;
        if (Array.isArray(casts) && casts.length > 0) {
            const typeByCol = new Map<string, DataType>();
            for (const c of casts) {
                const name = (c?.column ?? '').trim();
                const t = (c?.targetType ?? c?.type) as DataType | undefined;
                if (name && t) typeByCol.set(name, t);
            }
            if (typeByCol.size === 0) return up;
            return up.map(c =>
                typeByCol.has(c.name) ? { ...c, type: typeByCol.get(c.name)! } : c,
            );
        }
        // Legacy single-column cast.
        const col = props.column as string | undefined;
        const newType = props.targetType as DataType | undefined;
        if (!col || !newType) return up;
        return up.map(c => (c.name === col ? { ...c, type: newType } : c));
    }

    if (id === 'xf.addcol' || id === 'xf.coalesce') {
        const name = props.name as string | undefined;
        const type = (props.type as DataType | undefined) ?? 'string';
        const up = upstream();
        if (!name) return up;
        if (up.some(c => c.name === name)) return up;
        return [...up, { name, type, nullable: true }];
    }

    // #226: Text to Columns appends its parts and, with dropSource, removes the
    // column it split. Without this the node fell through to "schema unchanged",
    // so the new columns never reached the Schema or Preview tabs and the
    // dropped one stayed listed - showing as a column with no values, which is
    // exactly what it looks like when a schema describes a relation that does
    // not have it.
    //
    // The engine emits `SELECT *, nullif(split_part(...), '') AS name` - or
    // `SELECT * EXCLUDE (col), ...` when dropping - so the parts are text and
    // the source really is gone.
    if (id === 'xf.text.tocolumns') {
        const source = props.column as string | undefined;
        // The engine reads either name; the GUI writes outputColumns.
        const raw = (props.outputColumns ?? props.columns) as string | undefined;
        const names = String(raw ?? '')
            .split(',')
            .map(n => n.trim())
            .filter(Boolean);
        let up = upstream();
        if (!source || names.length === 0) return up;
        if (props.dropSource === true) {
            up = up.filter(c => c.name !== source);
        }
        const have = new Set(up.map(c => c.name));
        const added = names
            .filter(n => !have.has(n))
            .map(n => ({ name: n, type: 'string' as DataType, nullable: true }));
        return [...up, ...added];
    }

    if (id === 'xf.reorder') {
        const ordered = (props.columns as string[] | undefined) ?? [];
        const up = upstream();
        if (ordered.length === 0) return up;
        const colMap = new Map(up.map(c => [c.name, c] as const));
        const reordered: Column[] = ordered
            .map(n => colMap.get(n))
            .filter((c): c is Column => Boolean(c));
        const others = up.filter(c => !ordered.includes(c.name));
        return [...reordered, ...others];
    }

    if (id === 'xf.map') {
        const mapper = props.mapper as
            | { outputs?: Array<{ name: string; type: DataType }> }
            | undefined;
        if (Array.isArray(mapper?.outputs) && mapper.outputs.length > 0) {
            return mapper.outputs
                .filter((o): o is { name: string; type: DataType } => Boolean(o))
                .map(o => ({
                    name: o.name || 'col',
                    type: o.type,
                    nullable: true,
                }));
        }
        return node.data.schema ?? upstream();
    }

    if (id === 'xf.groupby') {
        const keys = (props.groupKeys as string[] | undefined) ?? [];
        const aggs = (props.aggregations as Aggregation[] | undefined) ?? [];
        const up = upstream();
        const keyCols = up.filter(c => keys.includes(c.name));
        const aggCols: Column[] = aggs.map(a => ({
            name: a.output || a.func + '_' + a.column,
            type: aggOutputType(a.func, up.find(c => c.name === a.column)),
            nullable: true,
        }));
        if (keyCols.length === 0 && aggCols.length === 0) return up;
        return [...keyCols, ...aggCols];
    }

    if (id?.startsWith('xf.window.') || (manifest?.id === 'xf.aggwin')) {
        const up = upstream();
        const output = (props.outputName as string | undefined) ?? 'window_result';
        if (up.some(c => c.name === output)) return up;
        return [...up, { name: output, type: 'int64', nullable: true }];
    }

    // Semi and anti joins filter the main input by EXISTS / NOT EXISTS, so no
    // lookup column reaches the output.
    if (id === 'xf.semi' || id === 'xf.semi.join' || id === 'xf.anti' || id === 'xf.anti.join') {
        return mainUpstream(node.id, nodes, edges, visiting);
    }

    if (id === 'xf.join' || id?.startsWith('xf.join.') || id === 'xf.lookup') {
        // Joins: union of all incoming schemas (driving + lookup).
        return mergedUpstream(node.id, nodes, edges, visiting);
    }

    if (id === 'xf.distinct' || id === 'xf.sort' || id === 'xf.filter' || id === 'xf.sample' || id === 'xf.topn' || id === 'xf.skip') {
        return upstream();
    }

    // Set ops - schema = column-name-union of inputs (approximate)
    if (id === 'xf.union' || id === 'xf.unionall' || id === 'xf.intersect' || id === 'xf.except') {
        return mergedUpstream(node.id, nodes, edges, visiting);
    }

    // Create Geometry (#206): with "remove source" on (the engine default) the
    // X/Y (or WKT/WKB) source columns are dropped and a geometry output column
    // is added. Mirrors build_geo_create so the Schema tab and downstream
    // inference match the engine, which emits SELECT * EXCLUDE(src), geom AS out.
    if (id === 'xf.geo.create') {
        const up = upstream();
        const source = (props.source as string) || 'xy';
        const outputName = (props.outputColumn as string) || 'geom';
        const removeSource = props.removeSource !== false;
        const sourceCols =
            source === 'wkt'
                ? [props.wktColumn as string]
                : source === 'wkb'
                  ? [props.wkbColumn as string]
                  : [props.xColumn as string, props.yColumn as string];
        const kept = removeSource ? up.filter(c => !sourceCols.includes(c.name)) : up;
        if (kept.some(c => c.name === outputName)) return kept;
        return [...kept, { name: outputName, type: 'geometry', nullable: true }];
    }

    // String / datetime / numeric / json / array - keep input, plus optional output
    if (
        id?.startsWith('xf.dt.') ||
        id?.startsWith('xf.num.') ||
        id?.startsWith('xf.json.') ||
        id?.startsWith('xf.arr.') ||
        (id?.startsWith('xf.') && id.split('.').length === 2)
    ) {
        const up = upstream();
        // #226: DuckDB's own answer, if it has been asked for this node yet.
        // It beats the guess below, which cannot see a transform that adds
        // several columns, removes one, or adds a column that is not text.
        const known = derived.get(derivedKey(node.id, id!, props, up));
        if (known) return known;
        const outputName = props.outputColumn as string | undefined;
        if (outputName && !up.some(c => c.name === outputName)) {
            return [...up, { name: outputName, type: 'string', nullable: true }];
        }
        return up;
    }

    // AI transforms (xf.ai.*) - 3-segment ids, so they miss the generic rule
    // above. Keep the input columns and add whatever each node emits.
    if (id?.startsWith('xf.ai.')) {
        const up = upstream();
        const out = props.outputColumn as string | undefined;
        if (id === 'xf.ai.chunk') {
            // explode mode adds the chunk text plus index/count metadata
            const name = out || 'chunk';
            const cols = [...up];
            if (!cols.some(c => c.name === name)) cols.push({ name, type: 'string', nullable: true });
            for (const meta of ['chunk_index', 'chunk_count']) {
                if (!cols.some(c => c.name === meta)) cols.push({ name: meta, type: 'int64', nullable: true });
            }
            return cols;
        }
        // pii / embed / classify / llm: optional new output column (else in-place).
        if (out && !up.some(c => c.name === out)) {
            const type: DataType = id === 'xf.ai.embed' ? 'json' : 'string';
            return [...up, { name: out, type, nullable: true }];
        }
        return up;
    }

    // Custom code - fall back to declared schema if any, otherwise pass through
    if (id?.startsWith('code.')) {
        return node.data.schema ?? upstream();
    }

    // CDC variants
    if (id?.startsWith('xf.cdc.')) {
        // changed output has full schema; reject/unchanged outputs same
        return upstream();
    }

    // Default - pass upstream through
    return upstream();
}

/**
 * #101: the columns an error-type reject adds after the row's own. A source's
 * or a quality check's reject port carries them; a filter's or a join's does
 * not, because that branch is the data going the other way, not an error.
 * The engine test `every_error_reject_port_carries_the_envelope` keeps the
 * `src.` / `qa.` rule true as components are added.
 */
const REJECT_ENVELOPE: Column[] = [
    { name: '__node_id', type: 'string', nullable: false },
    { name: '__error_code', type: 'string', nullable: false },
    { name: '__rejected_at', type: 'timestamp', nullable: false },
];

/** The schema an edge carries: its source's output, plus the envelope on an error reject. */
function edgeSchema(
    e: Edge,
    nodes: Node<DuckleNodeData>[],
    edges: Edge[],
    visiting: Set<string>,
): Column[] {
    const cols = resolveOutputSchema(e.source, nodes, edges, visiting);
    if (e.sourceHandle !== 'reject') return cols;
    const id = nodes.find(n => n.id === e.source)?.data.componentId ?? '';
    return id.startsWith('src.') || id.startsWith('qa.') ? [...cols, ...REJECT_ENVELOPE] : cols;
}

function mergedUpstream(
    nodeId: string,
    nodes: Node<DuckleNodeData>[],
    edges: Edge[],
    visiting: Set<string>,
): Column[] {
    const incoming = edges.filter(e => e.target === nodeId);
    if (incoming.length === 0) return [];
    const cols: Column[] = [];
    const seen = new Set<string>();
    for (const e of incoming) {
        const upSchema = edgeSchema(e, nodes, edges, visiting);
        for (const c of upSchema) {
            if (!seen.has(c.name)) {
                seen.add(c.name);
                cols.push(c);
            }
        }
    }
    return cols;
}

/** The schema arriving on the main input only, not on a lookup port. */
function mainUpstream(
    nodeId: string,
    nodes: Node<DuckleNodeData>[],
    edges: Edge[],
    visiting: Set<string>,
): Column[] {
    const main = edges.filter(e => e.target === nodeId && (e.targetHandle ?? 'main') === 'main');
    return main.length ? edgeSchema(main[0], nodes, edges, visiting) : [];
}

/**
 * Convenience for PropertiesPanel - schema flowing into this node.
 */
export function resolveUpstreamSchema(
    nodeId: string | undefined,
    nodes: Node<DuckleNodeData>[],
    edges: Edge[],
): Column[] {
    if (!nodeId) return [];
    return mergedUpstream(nodeId, nodes, edges, new Set());
}

/**
 * Per-input-port schemas - for components with multiple typed inputs
 * (mapper with main + lookups, joins with driving + lookup, etc.).
 */
export function resolveInputPortSchemas(
    nodeId: string,
    nodes: Node<DuckleNodeData>[],
    edges: Edge[],
): { portId: string; schema: Column[] }[] {
    const incoming = edges.filter(e => e.target === nodeId);
    const byPort = new Map<string, Column[]>();
    for (const e of incoming) {
        const portId = e.targetHandle ?? 'main';
        const arr = byPort.get(portId) ?? [];
        const sourceSchema = edgeSchema(e, nodes, edges, new Set());
        for (const c of sourceSchema) {
            if (!arr.some(x => x.name === c.name)) arr.push(c);
        }
        byPort.set(portId, arr);
    }
    return Array.from(byPort.entries()).map(([portId, schema]) => ({ portId, schema }));
}

/**
 * Find the closest upstream node (BFS) that carries a non-empty sample.
 */
export function resolveUpstreamSampleRows(
    nodeId: string | undefined,
    nodes: Node<DuckleNodeData>[],
    edges: Edge[],
): Record<string, unknown>[] {
    if (!nodeId) return [];
    const queue: string[] = edges.filter(e => e.target === nodeId).map(e => e.source);
    const visited = new Set<string>();
    while (queue.length > 0) {
        const id = queue.shift()!;
        if (visited.has(id)) continue;
        visited.add(id);
        const node = nodes.find(n => n.id === id);
        if (node?.data.sampleRows && node.data.sampleRows.length > 0) {
            return node.data.sampleRows;
        }
        for (const e of edges.filter(e => e.target === id)) queue.push(e.source);
    }
    return [];
}
