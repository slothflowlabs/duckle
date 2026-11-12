import { createContext } from 'react';
import type { Edge, Node } from '@xyflow/react';
import type { DuckleNodeData } from '../pipeline-types';

/**
 * The pipeline as it is, not as it is drawn. A collapsed group draws the links
 * that cross into it to its frame and leaves the ones inside it out, so a step
 * that worked out its columns from the drawn links would lose its input; it
 * reads these instead.
 */
export type PipelineGraph = { nodes: Node<DuckleNodeData>[]; edges: Edge[] };

export const PipelineGraphContext = createContext<PipelineGraph>({ nodes: [], edges: [] });
