import { useEffect, useMemo, useRef, useState, type MouseEvent as ReactMouseEvent } from 'react';
import { useTranslation } from 'react-i18next';
import type { Edge, Node } from '@xyflow/react';
import { CheckCircle2, ChevronLeft, ChevronRight, MousePointer2, Workflow } from 'lucide-react';
import { resolveUpstreamSchema, resolveUpstreamSampleRows, resolveOutputSchema } from '../schema-resolve';
import { buildContextVars, builtinVars, substituteDeep } from '../run-resolve';
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
        description: "PRAGMA memory_limit for this stage only. 0 = no override. NOTE: setting a per-stage limit forces slower per-stage execution (disables batching). For a pipeline-wide cap, set the DUCKLE_MEMORY_LIMIT workspace variable (e.g. 4GB) instead.",
    },
    {
        key: 'logRowCount',
        label: 'Log row count',
        kind: 'bool',
        defaultValue: false,
        description: 'Print the post-stage row count to the run output (descriptive; row counts already surface in node badges).',
    },
    {
        key: 'sqlOverride',
        label: 'Override stage SQL',
        kind: 'textarea',
        monospace: true,
        rows: 6,
        placeholder: 'SELECT * FROM input WHERE ...',
        description: "Advanced (source and transform stages): replace this stage's generated SQL with your own SELECT. Reference the upstream as `input`, or `input1`, `input2`, ... for multi-input stages like joins (main input first). Leave empty to use the generated SQL. A source stage with no upstream runs this SQL verbatim.",
    },
];

// Universal Materialize control, shown on the Basic tab of every node. The
// engine reads `materialize` off the node's properties.
const MATERIALIZE_FIELD: Field = {
    key: 'materialize',
    label: 'Materialize',
    kind: 'select',
    defaultValue: 'auto',
    options: [
        { label: 'Auto (view if one consumer, table if several)', value: 'auto' },
        { label: 'View (lazy, may re-scan the source)', value: 'view' },
        { label: 'Memory (read once, table held in RAM)', value: 'memory' },
        { label: 'Disk (read once, streamed via a temp Parquet file)', value: 'disk' },
        { label: 'DuckDB (read once, temp DuckDB file)', value: 'duckdb' },
        { label: 'DuckDB file (persistent - query it later)', value: 'duckdbfile' },
    ],
    description: 'How this step is stored. Auto uses a view for a single consumer and a table when several steps read it. Memory and Disk both read an expensive source only once (e.g. when a downstream split would otherwise scan it twice): Memory holds the rows as a table (fast, RAM-buffered), Disk streams them through a temp Parquet file (minimal RAM, for huge intermediates). DuckDB writes the step into a DuckDB database file - temporary (swept at run end) or a persistent named .duckdb you can open and query for analytics later. View keeps it lazy even with several consumers.',
};

// Shown only when Materialize = "DuckDB file (persistent)": the .duckdb path the
// step's rows are written to so they can be queried after the run.
const MATERIALIZE_PATH_FIELD: Field = {
    key: 'materializePath',
    label: 'DuckDB file path',
    kind: 'save-path',
    placeholder: '${workspace}/analytics/intermediate.duckdb',
    description: 'Absolute path (or ${workspace}-relative) to the .duckdb file this step is written into. The table is named after the node id.',
};

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
    workspacePath?: string | null;
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
    workspacePath,
    onUpdate,
    onOpenMapper,
    focusNameRequest,
}: Props) {
    const { t } = useTranslation();
    const [tab, setTab] = useState<TabId>('basic');
    const [autodetecting, setAutodetecting] = useState(false);
    const [detectError, setDetectError] = useState<string | null>(null);
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

    // Every component opens on the Basic tab. Switching to Schema / Preview /
    // Advanced / Validation is then the user's choice and stays put while this
    // component is selected; selecting a different node resets back to Basic.
    useEffect(() => {
        setTab('basic');
    }, [selected?.id]);

    const upstreamSchema = useMemo<Column[]>(
        () => resolveUpstreamSchema(selected?.id, allNodes, edges),
        [selected, edges, allNodes],
    );

    // #159: the Schema tab for an upstream-derived transform must show the node's
    // OWN output (post drop/rename/cast/...), not the raw input - otherwise a
    // Rename node's Schema tab keeps showing the old names while the Preview and
    // every downstream node already see the renamed ones. resolveOutputSchema is
    // the same per-component derivation downstream nodes already rely on.
    const outputSchema = useMemo<Column[]>(
        () => (selected ? resolveOutputSchema(selected.id, allNodes, edges) : []),
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

    // Right panel collapse: a thin rail on the right edge with an expand button,
    // so the canvas can use the full width. Persists per machine.
    const [collapsed, setCollapsed] = useState(
        () => localStorage.getItem('duckle.properties.collapsed') === '1',
    );
    const toggleCollapsed = () =>
        setCollapsed(c => {
            const next = !c;
            try {
                localStorage.setItem('duckle.properties.collapsed', next ? '1' : '0');
            } catch {
                /* localStorage unavailable - non-fatal */
            }
            return next;
        });

    // #102 item 4a: drag the panel's left edge to resize; width persists per machine.
    const [panelWidth, setPanelWidth] = useState<number>(() => {
        const saved = Number(localStorage.getItem('duckle.properties.width'));
        return saved >= 280 && saved <= 720 ? saved : 352;
    });
    const widthRef = useRef(panelWidth);
    const startResize = (e: ReactMouseEvent) => {
        e.preventDefault();
        const startX = e.clientX;
        const startW = widthRef.current;
        const onMove = (ev: MouseEvent) => {
            // Dragging the left edge: moving left widens the panel.
            const next = Math.max(280, Math.min(720, startW + (startX - ev.clientX)));
            widthRef.current = next;
            setPanelWidth(next);
        };
        const onUp = () => {
            try {
                localStorage.setItem('duckle.properties.width', String(widthRef.current));
            } catch {
                /* localStorage unavailable - non-fatal */
            }
            document.removeEventListener('mousemove', onMove);
            document.removeEventListener('mouseup', onUp);
        };
        document.addEventListener('mousemove', onMove);
        document.addEventListener('mouseup', onUp);
    };

    if (collapsed) {
        return (
            <aside className="properties properties-collapsed">
                <button
                    type="button"
                    className="properties-collapse-toggle"
                    onClick={toggleCollapsed}
                    title={t('properties.expandPanel', { defaultValue: 'Expand panel' })}
                    aria-label={t('properties.expandPanel', { defaultValue: 'Expand panel' })}
                >
                    <ChevronLeft size={16} />
                </button>
            </aside>
        );
    }

    if (!selected) {
        return (
            <aside className="properties" data-tour="properties" style={{ width: panelWidth }}>
                <div className="properties-resize-handle" onMouseDown={startResize} aria-hidden="true" />
                <button
                    type="button"
                    className="properties-collapse-toggle properties-collapse-toggle-float"
                    onClick={toggleCollapsed}
                    title={t('properties.collapsePanel', { defaultValue: 'Collapse panel' })}
                    aria-label={t('properties.collapsePanel', { defaultValue: 'Collapse panel' })}
                >
                    <ChevronRight size={16} />
                </button>
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
        setDetectError(null);
        try {
            // Resolve ${context} variables the same way the run path does, so a
            // context-bound value (e.g. a Path set to ${DUCKLE_PATH}) is
            // inspectable. Without this, autodetect sends the raw placeholder to
            // the engine and fails with "No files found that match the pattern
            // ${DUCKLE_PATH}" - the path's contents (including spaces) are
            // irrelevant; it is the unsubstituted placeholder that breaks.
            // ${ENV:...} secrets are resolved engine-side (the browser has no OS
            // env access), mirroring the run path (issue #148).
            const vars = { ...builtinVars(workspacePath), ...buildContextVars(repoItems) };
            const resolvedProps = substituteDeep(data.properties ?? {}, vars) as Record<string, unknown>;
            const result = await manifest.autodetect(resolvedProps);
            onUpdate(selected.id, {
                schema: result.columns,
                sampleRows: result.sampleRows,
            });
            // The engine reached the source but returned no columns: flag it
            // rather than leaving a silent (and previously fabricated) schema.
            if (result.columns.length === 0) {
                setDetectError(
                    'No columns detected. Check the connection details, or declare the schema manually below.',
                );
            }
        } catch (err) {
            // Surface the real reason (bad credentials, unreachable host,
            // unsupported source) instead of writing a fabricated schema (#148).
            setDetectError(err instanceof Error ? err.message : String(err));
        } finally {
            setAutodetecting(false);
        }
    };

    return (
        <aside className="properties" style={{ width: panelWidth }}>
            <div className="properties-resize-handle" onMouseDown={startResize} aria-hidden="true" />
            <button
                type="button"
                className="properties-collapse-toggle properties-collapse-toggle-float"
                onClick={toggleCollapsed}
                title={t('properties.collapsePanel', { defaultValue: 'Collapse panel' })}
                aria-label={t('properties.collapsePanel', { defaultValue: 'Collapse panel' })}
            >
                <ChevronRight size={16} />
            </button>
            <div className="properties-header">
                <div className="properties-kind-row">
                    <span
                        className="properties-kind-dot"
                        style={{ background: KIND_COLOR[kind] ?? '#666' }}
                        aria-hidden="true"
                    />
                    <span className="properties-kind">{KIND_LABEL[kind] ?? kind}</span>
                    <span className="properties-id" title={selected.id}>#{selected.id}</span>
                    {typeof data.alias === 'string' && data.alias.trim() ? (
                        <span className="properties-id" title="SQL name (alias)">
                            as {data.alias.trim()}
                        </span>
                    ) : null}
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
                    workspacePath,
                    activeContext,
                    nodeProps: props,
                    onPickConnection: (payload: ConnectionPayload) => {
                        if (!selected) return;
                        // #166 stage 2: Salesforce connections are ref-only.
                        // ConnectionRefField already stored the picked id as
                        // the node's connectionRef; the host resolves +
                        // decrypts it at RUN time, so credentials are never
                        // copied into the node props / pipeline JSON.
                        if (payload.kind === 'salesforce') return;
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
                            'endpoint',
                            'urlStyle',
                            // PostgreSQL advanced / libpq TLS options (#161).
                            'sslmode',
                            'sslrootcert',
                            'sslcert',
                            'sslkey',
                            'connectTimeout',
                            'options',
                            'connParams',
                        ];
                        for (const k of keys) {
                            const v = payload[k];
                            if (v !== undefined && v !== '' && v !== null) {
                                if (k === 'urlStyle' && typeof v === 'string') {
                                    // Normalize legacy free-text URL-style values
                                    // ('Path', 'Path (MinIO / B2)', 'Virtual host')
                                    // to the canonical node option value so the
                                    // node select shows the right choice (#116).
                                    const s = v.toLowerCase();
                                    next.urlStyle = s.startsWith('path')
                                        ? 'path'
                                        : s.startsWith('vhost') || s.includes('virtual')
                                          ? 'vhost'
                                          : '';
                                } else {
                                    next[k] = v as string | number;
                                }
                            }
                        }
                        // Snowflake components key the account identifier as
                        // `account`, but the connection stores it in `host`.
                        if (payload.kind === 'snowflake' && payload.host) {
                            next.account = payload.host;
                        }
                        onUpdate(selected.id, { properties: next });
                    },
                    onPickRoutine: (payload: RoutinePayload, routineId: string) => {
                        if (!selected) return;
                        // One update carrying the ref AND the inlined code, so a
                        // second update can't clobber routineRef off a stale base
                        // (issue #78). Inline into the field the component reads:
                        // code.sql / code.sqltemplate use `sql`, the other code.*
                        // use `code`.
                        const next = { ...(selected.data.properties ?? {}) };
                        next.routineRef = routineId;
                        const cid = selected.data.componentId;
                        const codeKey =
                            cid === 'code.sql' || cid === 'code.sqltemplate' ? 'sql' : 'code';
                        if (payload.code) next[codeKey] = payload.code;
                        if (payload.language) next.language = payload.language;
                        onUpdate(selected.id, { properties: next });
                    },
                }}
            >
                <div className="properties-content">
                    {tab === 'basic' ? (
                        <div className="properties-section">
                            <div style={{ marginBottom: 14 }}>
                                <label
                                    htmlFor="node-alias"
                                    style={{ display: 'block', fontWeight: 600, marginBottom: 4 }}
                                >
                                    {t('properties.sqlName', { defaultValue: 'SQL name' })}{' '}
                                    <span style={{ fontWeight: 400, opacity: 0.6 }}>
                                        {t('properties.optional', { defaultValue: '(optional)' })}
                                    </span>
                                </label>
                                <input
                                    id="node-alias"
                                    type="text"
                                    value={typeof data.alias === 'string' ? data.alias : ''}
                                    onChange={e =>
                                        onUpdate(selected.id, {
                                            alias: e.target.value.trim() ? e.target.value : undefined,
                                        })
                                    }
                                    placeholder={selected.id}
                                    spellCheck={false}
                                    autoComplete="off"
                                    style={{
                                        width: '100%',
                                        padding: '8px 10px',
                                        borderRadius: 8,
                                        border: '1px solid var(--border)',
                                        background: 'var(--bg-1)',
                                        color: 'inherit',
                                        boxSizing: 'border-box',
                                    }}
                                />
                                <p style={{ margin: '4px 0 0', fontSize: '0.8462rem', color: 'var(--text-3)' }}>
                                    {t('properties.sqlNameHelp', {
                                        defaultValue:
                                            'Reference this node by this name in Raw / Pure SQL nodes. Defaults to the node id.',
                                    })}
                                </p>
                            </div>
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
                                manifest.sections.map(section => {
                                    const fields = section.fields.map(field => (
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
                                    ));
                                    // Collapsible sections (e.g. Postgres advanced TLS, #161)
                                    // render as a native disclosure so the common case stays
                                    // clean. Open by default if any field already has a value.
                                    if (section.collapsible) {
                                        const hasValue = section.fields.some(
                                            f =>
                                                props[f.key] !== undefined &&
                                                props[f.key] !== '' &&
                                                props[f.key] !== null,
                                        );
                                        return (
                                            <details
                                                className="form-section form-section-collapsible"
                                                key={section.label}
                                                open={hasValue || !section.defaultCollapsed}
                                            >
                                                <summary className="form-section-label form-section-summary">
                                                    {section.label}
                                                </summary>
                                                {fields}
                                            </details>
                                        );
                                    }
                                    return (
                                        <div className="form-section" key={section.label}>
                                            <div className="form-section-label">{section.label}</div>
                                            {fields}
                                        </div>
                                    );
                                })
                            ) : (
                                <div className="properties-hint">
                                    {t('properties.genericComponent')}
                                </div>
                            )}
                            <div className="form-section">
                                <div className="form-section-label">Materialization</div>
                                <FieldRenderer
                                    field={
                                        // 'View' is meaningless on a terminal sink (it has no
                                        // downstream consumer and compiles to a COPY/driver
                                        // write), so hide that option for sink nodes only.
                                        kind === 'sink'
                                            ? {
                                                  ...MATERIALIZE_FIELD,
                                                  options: (MATERIALIZE_FIELD.options ?? []).filter(
                                                      o => o.value !== 'view'
                                                  ),
                                              }
                                            : MATERIALIZE_FIELD
                                    }
                                    value={
                                        props[MATERIALIZE_FIELD.key] !== undefined
                                            ? props[MATERIALIZE_FIELD.key]
                                            : MATERIALIZE_FIELD.defaultValue
                                    }
                                    onChange={v => setProperty(MATERIALIZE_FIELD.key, v)}
                                />
                                {props[MATERIALIZE_FIELD.key] === 'duckdbfile' ? (
                                    <FieldRenderer
                                        field={MATERIALIZE_PATH_FIELD}
                                        value={props[MATERIALIZE_PATH_FIELD.key]}
                                        onChange={v => setProperty(MATERIALIZE_PATH_FIELD.key, v)}
                                    />
                                ) : null}
                            </div>
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
                            {manifest?.schemaSource === 'autodetect' && detectError ? (
                                <div
                                    className="schema-autodetect-error"
                                    role="alert"
                                    style={{
                                        color: 'var(--color-error, #d64545)',
                                        marginTop: 6,
                                        fontSize: '0.85em',
                                        whiteSpace: 'pre-wrap',
                                    }}
                                >
                                    {detectError}
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
                                        ? outputSchema
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
