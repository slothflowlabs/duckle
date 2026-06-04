import { useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import type { Edge, Node } from '@xyflow/react';
import { CheckCircle2, MousePointer2, Workflow } from 'lucide-react';
import { resolveUpstreamSchema, resolveUpstreamSampleRows } from '../schema-resolve';
import type { Column, DuckleNodeData } from '../pipeline-types';
import type {
    ConnectionPayload,
    ContextPayload,
    RepoItem,
    RoutinePayload,
} from '../repo-types';
import SchemaEditor from './SchemaEditor';
import FieldRenderer from './fields/FieldRenderer';
import { FieldContext, type ActiveContext } from './fields/FieldContext';
import { getManifest } from './fields/component-manifests';
import type { Field } from './fields/types';

type TabId = 'basic' | 'schema' | 'preview' | 'advanced' | 'validation';

// Universal Advanced-tab fields. The engine reads retryAttempts /
// retryBackoffMs / memoryLimitMb directly off the node's properties;
// the other two are descriptive for now (no runtime wiring yet but
// surfaced so users can encode intent and avoid future churn).
const ADVANCED_FIELDS: Field[] = [
    {
        key: 'retryAttempts',
        label: 'Retry attempts',
        kind: 'integer',
        defaultValue: 1,
        description: 'Total attempts on failure (1 = no retry). The executor sleeps the backoff (linearly scaled by attempt index) between attempts.',
    },
    {
        key: 'retryBackoffMs',
        label: 'Retry backoff (ms)',
        kind: 'integer',
        defaultValue: 0,
        description: 'Sleep between retries; the Nth retry sleeps backoff * N milliseconds.',
    },
    {
        key: 'memoryLimitMb',
        label: 'Memory limit (MB)',
        kind: 'integer',
        defaultValue: 0,
        description: "PRAGMA memory_limit applied to this stage only. 0 = no override. Useful to cap a heavy aggregation without touching the whole pipeline.",
    },
    {
        key: 'logRowCount',
        label: 'Log row count',
        kind: 'bool',
        defaultValue: false,
        description: 'Print the post-stage row count to the run output (descriptive; row counts already surface in node badges).',
    },
];

const KIND_LABEL: Record<string, string> = {
    source: 'Source',
    transform: 'Transform',
    sink: 'Sink',
};

const KIND_COLOR: Record<string, string> = {
    source: '#2eafff',
    transform: '#3d8bff',
    sink: '#ff6900',
};

type Props = {
    selected: Node<DuckleNodeData> | null;
    allNodes: Node<DuckleNodeData>[];
    edges: Edge[];
    repoItems: RepoItem[];
    activeContextId?: string | null;
    onUpdate: (id: string, patch: Partial<DuckleNodeData>) => void;
    onOpenMapper?: (nodeId: string) => void;
    focusNameRequest?: number;
};

export default function PropertiesPanel({
    selected,
    allNodes,
    edges,
    repoItems,
    activeContextId,
    onUpdate,
    onOpenMapper,
    focusNameRequest,
}: Props) {
    const { t } = useTranslation();
    const [tab, setTab] = useState<TabId>('basic');
    const [autodetecting, setAutodetecting] = useState(false);
    const nameInputRef = useRef<HTMLInputElement>(null);

    useEffect(() => {
        if (focusNameRequest && nameInputRef.current) {
            setTab('basic');
            const el = nameInputRef.current;
            setTimeout(() => {
                el.focus();
                el.select();
            }, 50);
        }
    }, [focusNameRequest]);

    const upstreamSchema = useMemo<Column[]>(
        () => resolveUpstreamSchema(selected?.id, allNodes, edges),
        [selected, edges, allNodes],
    );

    const upstreamSampleRows = useMemo<Record<string, unknown>[]>(
        () => resolveUpstreamSampleRows(selected?.id, allNodes, edges),
        [selected, edges, allNodes],
    );

    const activeContext = useMemo<ActiveContext | undefined>(() => {
        if (!activeContextId) return undefined;
        const item = repoItems.find(r => r.id === activeContextId && r.type === 'context');
        if (!item) return undefined;
        const payload = item.payload as ContextPayload | undefined;
        return { id: item.id, name: item.name, variables: payload?.variables ?? [] };
    }, [activeContextId, repoItems]);

    if (!selected) {
        return (
            <aside className="properties">
                <div className="properties-empty">
                    <MousePointer2 size={32} strokeWidth={1.4} />
                    <div className="properties-empty-title">{t('properties.nothingSelected')}</div>
                    <div className="properties-empty-desc">
                        {t('properties.nothingSelectedDesc')}
                    </div>
                </div>
            </aside>
        );
    }

    const kind = (selected.type ?? 'transform') as string;
    const data = selected.data;
    const props = data.properties ?? {};
    const manifest = getManifest(data.componentId);
    const declaredSchema = data.schema ?? [];

    const TABS: { id: TabId; label: string }[] = [
        { id: 'basic', label: t('properties.tabBasic') },
        { id: 'schema', label: t('properties.tabSchema') },
        { id: 'preview', label: t('properties.tabPreview') },
        { id: 'advanced', label: t('properties.tabAdvanced') },
        { id: 'validation', label: t('properties.tabValidation') },
    ];

    const setLabel = (label: string) => onUpdate(selected.id, { label });
    const setProperty = (key: string, value: unknown) =>
        onUpdate(selected.id, { properties: { ...props, [key]: value } });
    const setSchema = (columns: Column[]) => onUpdate(selected.id, { schema: columns });

    const runAutodetect = async () => {
        if (!manifest?.autodetect) return;
        setAutodetecting(true);
        try {
            const result = await manifest.autodetect(data.properties ?? {});
            onUpdate(selected.id, {
                schema: result.columns,
                sampleRows: result.sampleRows,
            });
        } finally {
            setAutodetecting(false);
        }
    };

    return (
        <aside className="properties">
            <div className="properties-header">
                <div className="properties-kind-row">
                    <span
                        className="properties-kind-dot"
                        style={{ background: KIND_COLOR[kind] ?? '#666' }}
                        aria-hidden="true"
                    />
                    <span className="properties-kind">{KIND_LABEL[kind] ?? kind}</span>
                    <span className="properties-id">#{selected.id}</span>
                </div>
                <input
                    ref={nameInputRef}
                    type="text"
                    className="properties-name-input"
                    value={data.label}
                    onChange={e => setLabel(e.target.value)}
                    placeholder={t('properties.componentName')}
                    spellCheck={false}
                />
                {manifest ? (
                    <div className="properties-manifest-row">
                        <code className="properties-manifest-id">{manifest.id}</code>
                        {manifest.description ? (
                            <span className="properties-manifest-desc">{manifest.description}</span>
                        ) : null}
                    </div>
                ) : null}
            </div>

            <div className="properties-tabs" role="tablist">
                {TABS.map(t => (
                    <button
                        key={t.id}
                        type="button"
                        role="tab"
                        aria-selected={tab === t.id}
                        className="properties-tab"
                        onClick={() => setTab(t.id)}
                    >
                        {t.label}
                    </button>
                ))}
            </div>

            <FieldContext.Provider
                value={{
                    upstreamSchema,
                    nodeSchema: declaredSchema,
                    repoItems,
                    activeContext,
                    onPickConnection: (payload: ConnectionPayload) => {
                        if (!selected) return;
                        const next = { ...(selected.data.properties ?? {}) };
                        const keys: (keyof ConnectionPayload)[] = [
                            'host',
                            'port',
                            'database',
                            'username',
                            'password',
                            'bucket',
                            'region',
                            'accessKey',
                            'secretKey',
                            'accountName',
                            'accountKey',
                            'brokers',
                            'url',
                        ];
                        for (const k of keys) {
                            const v = payload[k];
                            if (v !== undefined && v !== '' && v !== null) {
                                next[k] = v as string | number;
                            }
                        }
                        onUpdate(selected.id, { properties: next });
                    },
                    onPickRoutine: (payload: RoutinePayload) => {
                        if (!selected) return;
                        const next = { ...(selected.data.properties ?? {}) };
                        if (payload.code) next.code = payload.code;
                        if (payload.language) next.language = payload.language;
                        onUpdate(selected.id, { properties: next });
                    },
                }}
            >
                <div className="properties-content">
                    {tab === 'basic' ? (
                        <div className="properties-section">
                            {data.componentId === 'xf.map' && onOpenMapper ? (
                                <button
                                    type="button"
                                    className="properties-mapper-button"
                                    onClick={() => onOpenMapper(selected.id)}
                                >
                                    <Workflow size={14} />
                                    {t('properties.openVisualMapper')}
                                </button>
                            ) : null}
                            {manifest ? (
                                manifest.sections.map(section => (
                                    <div className="form-section" key={section.label}>
                                        <div className="form-section-label">{section.label}</div>
                                        {section.fields.map(field => (
                                            <FieldRenderer
                                                key={field.key}
                                                field={field}
                                                value={
                                                    props[field.key] !== undefined
                                                        ? props[field.key]
                                                        : field.defaultValue
                                                }
                                                onChange={v => setProperty(field.key, v)}
                                            />
                                        ))}
                                    </div>
                                ))
                            ) : (
                                <div className="properties-hint">
                                    {t('properties.genericComponent')}
                                </div>
                            )}
                        </div>
                    ) : null}

                    {tab === 'schema' ? (
                        <div className="properties-section">
                            {manifest?.schemaSource === 'upstream' ? (
                                <div className="schema-source-banner">
                                    {t('properties.schemaInherited')}
                                </div>
                            ) : null}
                            {manifest?.schemaSource === 'autodetect' ? (
                                <div className="schema-autodetect-row">
                                    <button
                                        type="button"
                                        className="schema-autodetect-button"
                                        onClick={runAutodetect}
                                        disabled={autodetecting}
                                    >
                                        {autodetecting ? t('properties.detecting') : t('properties.autodetect')}
                                    </button>
                                    <span className="schema-autodetect-hint">
                                        {t('properties.autodetectHelp')}
                                    </span>
                                </div>
                            ) : null}
                            {manifest?.schemaSource === 'declared' ? (
                                <div className="schema-source-banner schema-source-banner-declared">
                                    {t('properties.declaredSchema')}
                                </div>
                            ) : null}
                            <SchemaEditor
                                columns={
                                    manifest?.schemaSource === 'upstream'
                                        ? upstreamSchema
                                        : declaredSchema
                                }
                                onChange={setSchema}
                                readOnly={manifest?.schemaSource === 'upstream'}
                            />
                        </div>
                    ) : null}

                    {tab === 'preview' ? (
                        <div className="properties-section">
                            <PreviewTab
                                schema={
                                    manifest?.schemaSource === 'upstream'
                                        ? upstreamSchema
                                        : declaredSchema.length > 0
                                          ? declaredSchema
                                          : upstreamSchema
                                }
                                rows={
                                    data.sampleRows && data.sampleRows.length > 0
                                        ? data.sampleRows
                                        : upstreamSampleRows
                                }
                                inheritedRows={
                                    (!data.sampleRows || data.sampleRows.length === 0) &&
                                    upstreamSampleRows.length > 0
                                }
                            />
                        </div>
                    ) : null}

                    {tab === 'advanced' ? (
                        <div className="properties-section">
                            <div className="form-section">
                                <div className="form-section-label">{t('properties.reliability')}</div>
                                {ADVANCED_FIELDS.map(field => (
                                    <FieldRenderer
                                        key={field.key}
                                        field={field}
                                        value={
                                            props[field.key] !== undefined
                                                ? props[field.key]
                                                : field.defaultValue
                                        }
                                        onChange={v => setProperty(field.key, v)}
                                    />
                                ))}
                            </div>
                        </div>
                    ) : null}

                    {tab === 'validation' ? (
                        <div className="properties-section">
                            <div className="validation-summary validation-ok">
                                <CheckCircle2 size={14} className="validation-icon" aria-hidden="true" />
                                <span>{t('properties.noIssues')}</span>
                            </div>
                            <div className="properties-hint">
                                {t('properties.noIssuesDesc')}
                            </div>
                        </div>
                    ) : null}
                </div>
            </FieldContext.Provider>
        </aside>
    );
}

type PreviewProps = {
    schema: Column[];
    rows: Record<string, unknown>[];
    inheritedRows?: boolean;
};

function PreviewTab({ schema, rows, inheritedRows }: PreviewProps) {
    const { t } = useTranslation();
    // When no formal schema resolved (e.g. a DB sink whose upstream schema
    // is empty until the source is read), derive columns from the sample
    // rows so the preview still renders from whatever data is available -
    // instead of being hidden behind "No schema".
    const effectiveSchema: Column[] =
        schema.length > 0
            ? schema
            : (() => {
                  const seen = new Set<string>();
                  const out: Column[] = [];
                  for (const r of rows) {
                      for (const k of Object.keys(r)) {
                          if (!seen.has(k)) {
                              seen.add(k);
                              out.push({ name: k, type: 'string', nullable: true });
                          }
                      }
                  }
                  return out;
              })();

    if (effectiveSchema.length === 0) {
        return (
            <div className="preview-empty">
                <div className="preview-empty-title">{t('properties.noSchema')}</div>
                <div className="preview-empty-desc">
                    {t('properties.noSchemaDesc')}
                </div>
            </div>
        );
    }

    if (rows.length === 0) {
        return (
            <div className="preview-empty">
                <div className="preview-empty-title">{t('properties.noSample')}</div>
                <div className="preview-empty-desc" dangerouslySetInnerHTML={{ __html: t('properties.noSampleDescHtml') }} />
            </div>
        );
    }

    const cols = effectiveSchema.map(c => c.name);
    return (
        <div className="preview-wrap">
            <div className="preview-meta">
                {rows.length} sample row{rows.length === 1 ? '' : 's'} · {cols.length} column
                {cols.length === 1 ? '' : 's'}
                {inheritedRows ? (
                    <span className="preview-meta-tag"> · upstream sample</span>
                ) : null}
            </div>
            <div className="preview-tablewrap">
                <table className="preview-table">
                    <thead>
                        <tr>
                            {effectiveSchema.map(c => (
                                <th key={c.name}>
                                    <div className="preview-th-name">{c.name}</div>
                                    <div className="preview-th-type">{c.type}</div>
                                </th>
                            ))}
                        </tr>
                    </thead>
                    <tbody>
                        {rows.map((r, i) => (
                            <tr key={i}>
                                {cols.map(name => (
                                    <td key={name}>{formatCell(r[name])}</td>
                                ))}
                            </tr>
                        ))}
                    </tbody>
                </table>
            </div>
        </div>
    );
}

function formatCell(v: unknown): string {
    if (v === null || v === undefined) return '∅';
    if (typeof v === 'object') return JSON.stringify(v);
    return String(v);
}
