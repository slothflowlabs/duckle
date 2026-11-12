import { useContext, useEffect, useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import {
    Handle,
    Position,
    useUpdateNodeInternals,
    type Node,
    type NodeProps,
} from '@xyflow/react';
import { AlertCircle, CheckCircle2, Loader2, XCircle } from 'lucide-react';
import type { DuckleNodeData } from '../../pipeline-types';
import { getManifest } from '../../workflow-ui/fields/component-manifests';
import { metaFor } from '../connection-types';
import type { PortDef } from '../../workflow-ui/fields/types';
import { resolveOutputSchema } from '../../schema-resolve';
import { PipelineGraphContext } from '../graph-context';
import { useRunStatus } from '../run-status-context';
import { deriveNodeSubtitle } from '../../node-subtitle';
import ComponentIcon from '../../workflow-ui/ComponentIcon';
import type { NodeKind } from '../../workflow-ui/palette-data';

export type DuckleFlowNode = Node<DuckleNodeData>;

export default function DuckleNode({ id, data, selected, type }: NodeProps<DuckleFlowNode>) {
    const { t } = useTranslation();
    const kind = type ?? 'transform';
    const manifest = getManifest(data.componentId);
    const ports = manifest?.ports;
    const inputs = ports?.inputs ?? [];
    const outputs = ports?.outputs ?? [];

    // The real pipeline, not the drawing: a collapsed group redraws links (graph-context.ts).
    const { nodes: allNodes, edges: allEdges } = useContext(PipelineGraphContext);

    // Parallelize branches are unlimited: instead of a fixed set of output
    // ports we grow them to one beyond the highest connected branch, so there
    // is always exactly one free branch to wire up next. The engine groups
    // branches dynamically by source handle, so nothing here is capped.
    const effectiveOutputs = useMemo(() => {
        if (data.componentId !== 'ctl.parallelize') return outputs;
        let maxConnected = 0;
        for (const e of allEdges) {
            if (e.source !== id) continue;
            const m = /^main_(\d+)$/.exec(e.sourceHandle ?? '');
            if (m) maxConnected = Math.max(maxConnected, Number(m[1]));
        }
        const count = Math.max(3, maxConnected + 1);
        return Array.from({ length: count }, (_, i): PortDef => {
            const n = i + 1;
            return { id: `main_${n}`, label: `branch ${n}`, type: 'main', optional: n > 2 };
        });
    }, [data.componentId, outputs, allEdges, id]);

    // React Flow caches handle positions per node; when the parallelize
    // node grows/shrinks its branch handles dynamically, we must tell React
    // Flow to remeasure or edges to the new handles won't render (they bind
    // to handle ids that did not exist at the node's first measure).
    const updateNodeInternals = useUpdateNodeInternals();
    useEffect(() => {
        if (data.componentId === 'ctl.parallelize') {
            updateNodeInternals(id);
        }
    }, [effectiveOutputs.length, id, data.componentId, updateNodeInternals]);

    const portCount = Math.max(inputs.length, effectiveOutputs.length);

    const effectiveSchema = useMemo(
        () => resolveOutputSchema(id, allNodes, allEdges),
        [id, allNodes, allEdges],
    );

    const needsConfig = useMemo(() => {
        if (!manifest) return false;
        const props = data.properties ?? {};
        for (const section of manifest.sections) {
            for (const field of section.fields) {
                if (!field.required) continue;
                const v = props[field.key];
                if (v === undefined || v === null || v === '') return true;
                if (Array.isArray(v) && v.length === 0) return true;
            }
        }
        return false;
    }, [manifest, data.properties]);

    const runStatus = useRunStatus(id);

    const classes =
        'node node-' + kind +
        (selected ? ' is-selected' : '') +
        (data.disabled ? ' is-disabled' : '') +
        (runStatus ? ' is-run-' + runStatus.status : '');

    return (
        <div className={classes}>
            <div className="node-header">
                <div className="node-header-row">
                    <div className="node-kind">{t(`node.${kind}`, { defaultValue: kind })}</div>
                    {runStatus ? <RunStatusBadge status={runStatus.status} /> : null}
                    {/* The rows a step produced, on the step itself, as Talend, Hop
                        and NiFi show them - not only in the Output tab. */}
                    {runStatus?.status === 'ok' && runStatus.rows !== undefined ? (
                        <span
                            className="node-run-rows"
                            title={
                                runStatus.duration_ms !== undefined
                                    ? t('node.rowsIn', {
                                          rows: runStatus.rows.toLocaleString(),
                                          unit: runStatus.rows === 1 ? t('node.rowsSingular') : t('node.rowsPlural'),
                                          ms: runStatus.duration_ms,
                                      })
                                    : undefined
                            }
                        >
                            {runStatus.rows.toLocaleString()}{' '}
                            {runStatus.rows === 1 ? t('node.rowsSingular') : t('node.rowsPlural')}
                        </span>
                    ) : null}
                    {needsConfig ? (
                        <span
                            className="node-needs-config"
                            title={t('node.needsConfig')}
                        >
                            <AlertCircle size={12} />
                        </span>
                    ) : null}
                </div>
                <div className="node-label-row">
                    <ComponentIcon
                        componentId={data.componentId ?? ''}
                        kind={kind as NodeKind}
                        size={16}
                        className="node-icon"
                    />
                    <div className="node-label" title={data.label}>{data.label}</div>
                </div>
                {(() => {
                    // Subtitle reflects ONLY the live config (file name,
                    // predicate, group-by keys, …). We intentionally don't
                    // fall back to a seeded subtitle, so a card never shows
                    // a label that isn't in the component's actual config.
                    const subtitle = deriveNodeSubtitle(data.componentId, data.properties);
                    return subtitle ? (
                        <div className="node-subtitle" title={subtitle}>
                            {subtitle}
                        </div>
                    ) : null;
                })()}
                {effectiveSchema.length > 0 ? (
                    <div className="node-schema-badge">
                        {effectiveSchema.length} {effectiveSchema.length === 1 ? t('node.colsSingular') : t('node.colsPlural')}
                    </div>
                ) : null}
                {data.disabled ? <div className="node-disabled-badge">{t('node.disabled')}</div> : null}
            </div>
            {portCount > 0 ? (
                <div className="node-ports">
                    <div className="node-ports-col node-ports-inputs">
                        {inputs.map(port => (
                            <PortRow key={port.id} port={port} side="input" />
                        ))}
                    </div>
                    <div className="node-ports-col node-ports-outputs">
                        {effectiveOutputs.map(port => (
                            <PortRow key={port.id} port={port} side="output" />
                        ))}
                    </div>
                </div>
            ) : null}
        </div>
    );
}

function RunStatusBadge({ status }: { status: 'running' | 'ok' | 'error' }) {
    const { t } = useTranslation();
    if (status === 'running') {
        return (
            <span className="node-run-badge node-run-badge-running" title={t('node.running')}>
                <Loader2 size={11} />
            </span>
        );
    }
    if (status === 'error') {
        return (
            <span className="node-run-badge node-run-badge-error" title={t('node.failed')}>
                <XCircle size={11} />
            </span>
        );
    }
    return (
        <span className="node-run-badge node-run-badge-ok" title={t('node.ok')}>
            <CheckCircle2 size={11} />
        </span>
    );
}

function PortRow({ port, side }: { port: PortDef; side: 'input' | 'output' }) {
    const meta = metaFor(port.type);
    const isInput = side === 'input';

    return (
        <div
            className={
                'node-port node-port-' +
                side +
                ' node-port-type-' +
                port.type +
                (port.optional ? ' is-optional' : '')
            }
            title={meta.label + ' · ' + meta.description}
        >
            {isInput ? (
                <Handle
                    type="target"
                    position={Position.Left}
                    id={port.id}
                    className="node-port-handle"
                    style={{ background: meta.color, borderColor: 'var(--bg-1)' }}
                />
            ) : null}
            {isInput ? (
                <>
                    <span
                        className="node-port-dot"
                        style={{ background: meta.color }}
                        aria-hidden="true"
                    />
                    <span className="node-port-label">{port.label}</span>
                </>
            ) : (
                <>
                    <span className="node-port-label">{port.label}</span>
                    <span
                        className="node-port-dot"
                        style={{ background: meta.color }}
                        aria-hidden="true"
                    />
                </>
            )}
            {!isInput ? (
                <Handle
                    type="source"
                    position={Position.Right}
                    id={port.id}
                    className="node-port-handle"
                    style={{ background: meta.color, borderColor: 'var(--bg-1)' }}
                />
            ) : null}
        </div>
    );
}
