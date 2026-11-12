import {
    BaseEdge,
    EdgeLabelRenderer,
    getBezierPath,
    type Edge,
    type EdgeProps,
} from '@xyflow/react';
import { metaFor, type ConnectionType } from './connection-types';
import { useRunStatus } from './run-status-context';

export type DuckleEdgeData = {
    connectionType: ConnectionType;
    label?: string;
    condition?: string;
    /** A SQL condition an editor run stops at when a row going down this link matches it. */
    breakpoint?: string;
};

export type DuckleEdgeType = Edge<DuckleEdgeData, 'duckle'>;

export default function DuckleEdge(props: EdgeProps<DuckleEdgeType>) {
    const {
        id,
        source,
        target,
        sourceX,
        sourceY,
        targetX,
        targetY,
        sourcePosition,
        targetPosition,
        data,
        selected,
        markerEnd,
    } = props;

    const [path, labelX, labelY] = getBezierPath({
        sourceX,
        sourceY,
        sourcePosition,
        targetX,
        targetY,
        targetPosition,
    });

    const type = (data?.connectionType ?? 'main') as ConnectionType;
    const meta = metaFor(type);
    const showLabel = Boolean(meta.badge || data?.label || data?.condition || data?.breakpoint);

    const sourceStatus = useRunStatus(source);
    const targetStatus = useRunStatus(target);
    const isFlowing =
        sourceStatus?.status === 'running' || targetStatus?.status === 'running';
    const sourceDone =
        sourceStatus?.status === 'ok' || sourceStatus?.status === 'error';
    const targetDone =
        targetStatus?.status === 'ok' || targetStatus?.status === 'error';
    const isCompleted = sourceDone && targetDone;

    return (
        <>
            <BaseEdge
                id={id}
                path={path}
                markerEnd={markerEnd}
                className={
                    'duckle-edge' +
                    (isFlowing ? ' is-flowing' : '') +
                    (isCompleted ? ' is-completed' : '')
                }
                style={{
                    stroke: selected ? 'var(--accent-strong)' : meta.color,
                    strokeDasharray: isFlowing
                        ? '8 4'
                        : meta.dash ?? undefined,
                    strokeWidth: selected ? meta.width + 0.6 : meta.width,
                    filter: selected ? 'drop-shadow(0 0 6px var(--accent-glow))' : undefined,
                    opacity: isCompleted && !selected ? 0.55 : undefined,
                }}
            />
            {showLabel ? (
                <EdgeLabelRenderer>
                    <div
                        className={
                            'edge-label edge-label-' +
                            meta.group +
                            ' edge-label-' +
                            type +
                            (selected ? ' is-selected' : '')
                        }
                        style={{
                            position: 'absolute',
                            transform: `translate(-50%, -50%) translate(${labelX}px, ${labelY}px)`,
                            pointerEvents: 'all',
                        }}
                    >
                        {data?.label ? <span className="edge-label-name">{data.label}</span> : null}
                        {meta.badge ? <span className="edge-label-badge">{meta.badge}</span> : null}
                        {data?.condition ? (
                            <span className="edge-label-cond" title={data.condition}>
                                {truncate(data.condition, 40)}
                            </span>
                        ) : null}
                        {data?.breakpoint ? (
                            <span
                                className="edge-label-breakpoint"
                                title={'Breakpoint: an editor run stops here when a row matches ' + data.breakpoint}
                            >
                                {truncate(data.breakpoint, 32)}
                            </span>
                        ) : null}
                    </div>
                </EdgeLabelRenderer>
            ) : null}
        </>
    );
}

function truncate(s: string, max: number): string {
    if (s.length <= max) return s;
    return s.slice(0, max - 1) + '…';
}
