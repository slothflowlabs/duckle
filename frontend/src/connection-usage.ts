import type { Node } from '@xyflow/react';
import type { DuckleNodeData } from './pipeline-types';
import type { RepoItem } from './repo-types';
import { getManifest } from './workflow-ui/fields/component-manifests';

/**
 * The pipelines a delete would leave pointing at a connection that is gone: those
 * outside `deleting` with a node whose connection field names a connection inside
 * it. By name, in repository order.
 *
 * A connection field is any field of kind `connection-ref` in the node's form, so
 * an HTTP transport (`transportRef`) counts as well as `connectionRef`.
 */
export function pipelinesUsingConnections(
    deleting: ReadonlySet<string>,
    repo: RepoItem[],
    pipelineData: Record<string, { nodes?: Node<DuckleNodeData>[] } | undefined>,
): string[] {
    const gone = new Set(repo.filter(i => i.type === 'connection' && deleting.has(i.id)).map(i => i.id));
    if (gone.size === 0) return [];
    const usesOne = (node: Node<DuckleNodeData>) => {
        const props = node.data?.properties ?? {};
        return (getManifest(node.data?.componentId)?.sections ?? []).some(section =>
            section.fields.some(f => f.kind === 'connection-ref' && gone.has(String(props[f.key] ?? ''))),
        );
    };
    return repo
        .filter(i => i.type === 'pipeline' && !deleting.has(i.id))
        .filter(i => (pipelineData[i.id]?.nodes ?? []).some(usesOne))
        .map(i => i.name);
}
