// "Save as sub-flow": the selected steps move into a new pipeline, reached
// through Sub-flow input and output nodes, and one Sub-flow node takes their
// place. Pure, so check-logic can hold it to its word.

type FlowNode = {
    id: string;
    type?: string;
    position: { x: number; y: number };
    data: { label?: string; componentId?: string; properties?: Record<string, unknown>; [key: string]: unknown };
    [key: string]: unknown;
};

type FlowEdge = {
    id: string;
    source: string;
    target: string;
    sourceHandle?: string | null;
    targetHandle?: string | null;
    data?: Record<string, unknown>;
    [key: string]: unknown;
};

type Pipeline = { nodes: FlowNode[]; edges: FlowEdge[]; [key: string]: unknown };

export type Extracted = { subflow: Pipeline; caller: Pipeline; callId: string };

const handleOf = (h: string | null | undefined): string => (h ? h : 'main');

/** Edges grouped by the step and port they leave, in the order first seen. */
function bySource(edges: FlowEdge[]): FlowEdge[][] {
    const groups = new Map<string, FlowEdge[]>();
    for (const e of edges) {
        const key = `${e.source}|${handleOf(e.sourceHandle)}`;
        groups.set(key, [...(groups.get(key) ?? []), e]);
    }
    return [...groups.values()];
}

/** The first port is "main", then `<rest>_2`, `<rest>_3`, ... */
const portName = (i: number, rest: string): string => (i === 0 ? 'main' : `${rest}_${i + 1}`);

const link = (id: string, source: string, sourceHandle: string, target: string, targetHandle: string, kind = 'main'): FlowEdge => ({
    id,
    type: 'duckle',
    source,
    sourceHandle,
    target,
    targetHandle,
    data: { connectionType: kind },
});

/**
 * Split `nodeIds` out of `pipeline` into a sub-flow called `subflowId`. Each
 * outside step (and port) that feeds the selection becomes one input; each
 * selected step (and port) that feeds the outside becomes one output. Null
 * when nothing is selected.
 */
export function extractSubflow(pipeline: Pipeline, nodeIds: string[], subflowId: string): Extracted | null {
    const inside = new Set(nodeIds);
    const chosen = pipeline.nodes.filter(n => inside.has(n.id));
    if (chosen.length === 0) return null;
    const taken = new Set(pipeline.nodes.map(n => n.id));
    const fresh = (base: string): string => {
        let id = base;
        for (let n = 2; taken.has(id); n++) id = `${base}_${n}`;
        taken.add(id);
        return id;
    };

    const crossingIn = pipeline.edges.filter(e => !inside.has(e.source) && inside.has(e.target));
    const crossingOut = pipeline.edges.filter(e => inside.has(e.source) && !inside.has(e.target));
    const internal = pipeline.edges.filter(e => inside.has(e.source) && inside.has(e.target));
    const ins = bySource(crossingIn);
    const outs = bySource(crossingOut);

    const xs = chosen.map(n => n.position.x);
    const ys = chosen.map(n => n.position.y);
    const [left, right, top] = [Math.min(...xs), Math.max(...xs), Math.min(...ys)];
    const boundary = (id: string, componentId: string, port: string, x: number, y: number): FlowNode => ({
        id,
        type: 'transform',
        position: { x, y },
        data: {
            label: componentId === 'ctl.subflow.in' ? `In: ${port}` : `Out: ${port}`,
            componentId,
            properties: { port },
        },
    });

    const subNodes: FlowNode[] = chosen.map(n => ({ ...n, selected: false }));
    const subEdges: FlowEdge[] = [...internal];
    const callId = fresh('subflow');
    const callerEdges: FlowEdge[] = pipeline.edges.filter(e => !inside.has(e.source) && !inside.has(e.target));

    ins.forEach((group, i) => {
        const port = portName(i, 'in');
        const inId = `in_${i + 1}`;
        subNodes.push(boundary(inId, 'ctl.subflow.in', port, left - 300, top + i * 140));
        for (const e of group) {
            const kind = typeof e.data?.connectionType === 'string' ? e.data.connectionType : 'main';
            subEdges.push(link(`${inId}_${e.id}`, inId, 'main', e.target, handleOf(e.targetHandle), kind));
        }
        const first = group[0];
        callerEdges.push(link(`${callId}_in_${i + 1}`, first.source, handleOf(first.sourceHandle), callId, port));
    });
    outs.forEach((group, i) => {
        const port = portName(i, 'out');
        const outId = `out_${i + 1}`;
        subNodes.push(boundary(outId, 'ctl.subflow.out', port, right + 300, top + i * 140));
        const first = group[0];
        subEdges.push(link(`${outId}_link`, first.source, handleOf(first.sourceHandle), outId, 'main'));
        for (const e of group) {
            callerEdges.push({ ...e, source: callId, sourceHandle: port });
        }
    });

    const call: FlowNode = {
        id: callId,
        type: 'transform',
        position: { x: (left + right) / 2, y: top },
        data: { label: 'Sub-flow', componentId: 'ctl.subflow', properties: { subflowRef: subflowId } },
    };
    return {
        subflow: { nodes: subNodes, edges: subEdges },
        caller: { ...pipeline, nodes: [...pipeline.nodes.filter(n => !inside.has(n.id)), call], edges: callerEdges },
        callId,
    };
}
