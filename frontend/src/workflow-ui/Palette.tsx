import { useMemo, useState, useSyncExternalStore, type DragEvent } from 'react';
import { useTranslation } from 'react-i18next';
import {
    ArrowDownToLine,
    ArrowUpFromLine,
    Check,
    ChevronDown,
    ChevronRight,
    Cloud,
    Code2,
    GitFork,
    Search,
    ShieldCheck,
    Workflow,
    X,
} from 'lucide-react';
import {
    PALETTE,
    paletteWith,
    subscribeExternalComponents,
    getExternalComponents,
    TOTAL_COMPONENT_COUNT,
    AVAILABLE_COUNT,
    type ComponentDef,
} from './palette-data';
import ComponentIcon from './ComponentIcon';

const CATEGORY_ICONS: Record<string, React.ReactNode> = {
    sources: <ArrowDownToLine size={13} />,
    transforms: <Workflow size={13} />,
    sinks: <ArrowUpFromLine size={13} />,
    control: <GitFork size={13} />,
    quality: <ShieldCheck size={13} />,
    code: <Code2 size={13} />,
    saas: <Cloud size={13} />,
};

const DEFAULT_EXPANDED = new Set<string>();
const ALL_CATEGORY_IDS = PALETTE.map(c => c.id);

// Map palette top-level category IDs to i18n keys under "palette.*".
const CAT_LABEL_KEY: Record<string, string> = {
    sources: 'palette.sources',
    transforms: 'palette.transforms',
    sinks: 'palette.sinks',
    control: 'palette.controlFlow',
    quality: 'palette.dataQuality',
    code: 'palette.customCode',
};

// Map subgroup English labels to i18n keys under "palette.groups.*".
// Labels that appear in multiple categories ("Files" in src + snk,
// "Databases" in src + snk + etc) share one key by design - the
// translation is the same word in every category context.
const GROUP_LABEL_KEY: Record<string, string> = {
    'Files': 'palette.groups.files',
    'Lakehouse table formats': 'palette.groups.lakehouse',
    'Databases': 'palette.groups.databases',
    'Cloud Warehouses': 'palette.groups.cloudWarehouses',
    'Object Storage': 'palette.groups.objectStorage',
    'Streaming': 'palette.groups.streaming',
    'APIs': 'palette.groups.apis',
    'NoSQL & Search': 'palette.groups.nosqlSearch',
    'Other': 'palette.groups.other',
    'Vector / AI Databases': 'palette.groups.vectorAi',
    'Fields': 'palette.groups.fields',
    'Rows': 'palette.groups.rows',
    'Aggregate': 'palette.groups.aggregate',
    'Join': 'palette.groups.join',
    'Set Operations': 'palette.groups.setOps',
    'Window': 'palette.groups.window',
    'Strings': 'palette.groups.strings',
    'Date / Time': 'palette.groups.dateTime',
    'Numeric': 'palette.groups.numeric',
    'Pivot / Shape': 'palette.groups.pivotShape',
    'JSON / Nested': 'palette.groups.jsonNested',
    'Array': 'palette.groups.array',
    'CDC / SCD': 'palette.groups.cdcScd',
    'AI': 'palette.groups.ai',
    'Geospatial': 'palette.groups.geospatial',
    'Debug': 'palette.groups.debug',
    'Routing': 'palette.groups.routing',
    'Timing': 'palette.groups.timing',
    'Pipelines': 'palette.groups.pipelines',
    'Error Handling': 'palette.groups.errorHandling',
    'Validation': 'palette.groups.validation',
    'Profiling': 'palette.groups.profiling',
    'Cleansing': 'palette.groups.cleansing',
    'SQL': 'palette.groups.sql',
    'Scripting': 'palette.groups.scripting',
    'CRM': 'palette.groups.crm',
    'Finance': 'palette.groups.finance',
    'Productivity': 'palette.groups.productivity',
    'Dev Tools': 'palette.groups.devTools',
    'Marketing': 'palette.groups.marketing',
    'Communication': 'palette.groups.communication',
};

export default function Palette() {
    const { t } = useTranslation();
    const [query, setQuery] = useState('');
    const [expanded, setExpanded] = useState<Set<string>>(DEFAULT_EXPANDED);

    const q = query.trim().toLowerCase();

    // #307: external components appear when a workspace declares them.
    const external = useSyncExternalStore(subscribeExternalComponents, getExternalComponents);
    const categories = useMemo(() => paletteWith(external), [external]);

    const filtered = useMemo(() => {
        if (!q) return categories;
        return categories.map(cat => ({
            ...cat,
            groups: cat.groups
                .map(g => ({
                    ...g,
                    components: g.components.filter(
                        c =>
                            c.label.toLowerCase().includes(q) ||
                            c.id.toLowerCase().includes(q) ||
                            (c.summary?.toLowerCase().includes(q) ?? false),
                    ),
                }))
                .filter(g => g.components.length > 0),
        })).filter(cat => cat.groups.length > 0);
    }, [q]);

    const toggle = (id: string) => {
        setExpanded(s => {
            const next = new Set(s);
            if (next.has(id)) next.delete(id);
            else next.add(id);
            return next;
        });
    };

    const onDragStart = (e: DragEvent<HTMLDivElement>, c: ComponentDef) => {
        e.dataTransfer.setData('application/duckle-component', JSON.stringify(c));
        e.dataTransfer.effectAllowed = 'copy';
    };

    return (
        <aside className="palette">
            <div className="palette-header">
                <div className="palette-search-wrap">
                    <Search className="palette-search-icon" size={14} aria-hidden="true" />
                    <input
                        type="text"
                        className="palette-search"
                        placeholder={t('palette.searchPlaceholder')}
                        value={query}
                        onChange={e => setQuery(e.target.value)}
                        spellCheck={false}
                    />
                    {query ? (
                        <button
                            type="button"
                            className="palette-search-clear"
                            onClick={() => setQuery('')}
                            aria-label={t('palette.clearSearch')}
                        >
                            <X size={12} />
                        </button>
                    ) : null}
                </div>
                <div className="palette-stats">
                    <span>
                        <b>{AVAILABLE_COUNT}</b> {t('palette.available')}
                    </span>
                    <span className="palette-stats-sep">·</span>
                    <span>
                        <b>{TOTAL_COMPONENT_COUNT}</b> {t('palette.total')}
                    </span>
                    <span className="palette-stats-spacer" />
                    <button
                        type="button"
                        className="palette-stats-btn"
                        onClick={() => setExpanded(new Set(ALL_CATEGORY_IDS))}
                        title={t('palette.expandAllTooltip')}
                    >
                        {t('palette.expandAll')}
                    </button>
                    <button
                        type="button"
                        className="palette-stats-btn"
                        onClick={() => setExpanded(new Set())}
                        title={t('palette.collapseAllTooltip')}
                    >
                        {t('palette.collapseAll')}
                    </button>
                </div>
            </div>

            <div className="palette-body">
                {filtered.length === 0 ? (
                    <div className="palette-empty">
                        {t('palette.noMatch')} <span className="quote">{query}</span>
                    </div>
                ) : (
                    filtered.map(cat => {
                        const isExpanded = !!q || expanded.has(cat.id);
                        const count = cat.groups.reduce((acc, g) => acc + g.components.length, 0);
                        return (
                            <div className="palette-category" key={cat.id}>
                                <button
                                    type="button"
                                    className="palette-category-header"
                                    aria-expanded={isExpanded}
                                    onClick={() => toggle(cat.id)}
                                >
                                    <span className="palette-cat-chevron" aria-hidden="true">
                                        {isExpanded ? (
                                            <ChevronDown size={12} />
                                        ) : (
                                            <ChevronRight size={12} />
                                        )}
                                    </span>
                                    <span className="palette-cat-icon" aria-hidden="true">
                                        {CATEGORY_ICONS[cat.id]}
                                    </span>
                                    <span className="palette-cat-label">{CAT_LABEL_KEY[cat.id] ? t(CAT_LABEL_KEY[cat.id]) : cat.label}</span>
                                    <span className="palette-cat-count">{count}</span>
                                </button>
                                {isExpanded ? (
                                    <div className="palette-category-body">
                                        {cat.groups.map(g => (
                                            <div className="palette-group" key={g.id}>
                                                <div className="palette-group-label">{GROUP_LABEL_KEY[g.label] ? t(GROUP_LABEL_KEY[g.label]) : g.label}</div>
                                                {g.components.map(c => {
                                                    const availClass =
                                                        c.availability === 'planned'
                                                            ? ' is-planned'
                                                            : c.availability === 'preview'
                                                              ? ' is-preview'
                                                              : ' is-available';
                                                    const badgeLabel =
                                                        c.availability === 'preview'
                                                            ? 'Preview'
                                                            : c.availability === 'planned'
                                                              ? 'Planned'
                                                              : null;
                                                    const titleParts = [
                                                        c.summary ?? c.label,
                                                        c.alternateHint,
                                                        badgeLabel
                                                            ? `(${badgeLabel} - not executable on DuckDB engine yet)`
                                                            : null,
                                                    ].filter(Boolean);
                                                    return (
                                                    <div
                                                        key={c.id}
                                                        className={
                                                            'palette-component' + availClass
                                                        }
                                                        draggable={c.availability === 'available'}
                                                        onDragStart={
                                                            c.availability === 'available'
                                                                ? e => onDragStart(e, c)
                                                                : undefined
                                                        }
                                                        title={titleParts.join(' · ')}
                                                    >
                                                        <ComponentIcon
                                                            componentId={c.id}
                                                            kind={c.kind}
                                                            size={15}
                                                            className="palette-component-icon"
                                                        />
                                                        <span className="palette-component-label">
                                                            {c.label}
                                                        </span>
                                                        {badgeLabel ? (
                                                            <span
                                                                className={
                                                                    'palette-badge palette-badge-' +
                                                                    c.availability
                                                                }
                                                            >
                                                                {badgeLabel}
                                                            </span>
                                                        ) : c.availability === 'available' ? (
                                                            <Check
                                                                className="palette-availability palette-availability-yes"
                                                                size={12}
                                                                aria-label="available"
                                                            />
                                                        ) : (
                                                            <span
                                                                className="palette-availability palette-availability-no"
                                                                aria-label={c.availability}
                                                            />
                                                        )}
                                                    </div>
                                                    );
                                                })}
                                            </div>
                                        ))}
                                    </div>
                                ) : null}
                            </div>
                        );
                    })
                )}
            </div>
        </aside>
    );
}
