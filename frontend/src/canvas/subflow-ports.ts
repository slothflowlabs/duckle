// The ports a Sub-flow node (ctl.subflow) shows: one per Sub-flow input and
// output node of the pipeline it runs, by port name. The engine wires the
// node's links by those names when it expands the call (subflow.rs).
import { createContext } from 'react';

export type SubflowPorts = { inputs: string[]; outputs: string[] };

type PipelineLike = {
    nodes?: Array<{ data?: { componentId?: string; properties?: Record<string, unknown> } }>;
};

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

/** Looks a sub-flow's ports up by its pipeline id; null when it is not loaded. */
export const SubflowPortsContext = createContext<(pipelineId: string) => SubflowPorts | null>(() => null);
