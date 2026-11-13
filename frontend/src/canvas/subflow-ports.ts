// The ports a Sub-flow node (ctl.subflow) shows: one per Sub-flow input and
// output node of the pipeline it runs, by port name. The engine wires the
// node's links by those names when it expands the call (subflow.rs).
import { createContext } from 'react';

export type SubflowPorts = { inputs: string[]; outputs: string[] };

type StepLike = { id?: string; data?: { label?: string; componentId?: string; properties?: Record<string, unknown> } };

type PipelineLike = { nodes?: StepLike[] };

/** A boundary node's port: its `port`, "main" when unset - as the engine reads it. */
function portOf(properties: Record<string, unknown> | undefined): string {
    const p = properties?.port;
    return typeof p === 'string' && p.trim() ? p.trim() : 'main';
}

/** The ports a pipeline offers when it runs as a sub-flow; null when it is not loaded. */
export function subflowPorts(pipeline: PipelineLike | undefined | null): SubflowPorts | null {
    if (!pipeline?.nodes) return null;
    const of = (componentId: string) =>
        pipeline.nodes!.filter(n => n.data?.componentId === componentId).map(n => portOf(n.data?.properties));
    return { inputs: of('ctl.subflow.in'), outputs: of('ctl.subflow.out') };
}

/**
 * Labels for the steps a run reports that the canvas does not show. A Sub-flow
 * node's steps run as `<node>__<step>`, labelled "<node> / <step>", and a
 * nested sub-flow's the same way again: the names the engine gives them when it
 * expands the call (subflow.rs). Sub-flows not loaded here are left out.
 */
export function subflowStepLabels(
    nodes: StepLike[],
    pipelineOf: (pipelineId: string) => PipelineLike | null | undefined,
    depth = 0,
): Record<string, string> {
    const out: Record<string, string> = {};
    if (depth > 16) return out; // the engine refuses nesting this deep
    for (const call of nodes) {
        const ref = call.data?.componentId === 'ctl.subflow' ? call.data.properties?.subflowRef : undefined;
        const sub = typeof ref === 'string' && ref ? pipelineOf(ref) : null;
        if (!call.id || !sub?.nodes) continue;
        const name = call.data?.label ?? call.id;
        for (const step of sub.nodes) {
            const kind = step.data?.componentId;
            if (!step.id || kind === 'ctl.subflow.in' || kind === 'ctl.subflow.out') continue;
            out[`${call.id}__${step.id}`] = `${name} / ${step.data?.label ?? step.id}`;
        }
        for (const [id, label] of Object.entries(subflowStepLabels(sub.nodes, pipelineOf, depth + 1))) {
            out[`${call.id}__${id}`] = `${name} / ${label}`;
        }
    }
    return out;
}

/** Looks a sub-flow's ports up by its pipeline id; null when it is not loaded. */
export const SubflowPortsContext = createContext<(pipelineId: string) => SubflowPorts | null>(() => null);
