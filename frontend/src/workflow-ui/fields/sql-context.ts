/**
 * #314: what the SQL field needs to know to ask for completions.
 *
 * A small store rather than a prop threaded through `FieldRenderer`: that
 * component renders twenty field kinds and none of the others want a pipeline.
 * Set when a node is selected, read when its SQL field asks.
 */
import type { Edge, Node } from '@xyflow/react';
import type { Column, DuckleNodeData } from '../../pipeline-types';

export type SqlContext = {
    nodes: Node<DuckleNodeData>[];
    edges: Edge[];
    nodeId: string;
    inputs: Array<[string, Column[]]>;
};

let current: SqlContext | null = null;

export function setSqlContext(ctx: SqlContext | null): void {
    current = ctx;
}

export function sqlContext(): SqlContext | null {
    return current;
}
