import { useMemo, useState, type DragEvent } from 'react';
import {
    PALETTE,
    TOTAL_COMPONENT_COUNT,
    AVAILABLE_COUNT,
    type ComponentDef,
    type NodeKind,
} from './palette-data';

const KIND_COLOR: Record<NodeKind, string> = {
    source: '#7ee787',
    transform: '#58a6ff',
    sink: '#ffa657',
    control: '#c39bff',
    quality: '#fed060',
    custom: '#ff7b72',
};

const DEFAULT_EXPANDED = new Set<string>(['sources', 'transforms', 'sinks']);

export default function Palette() {
    const [query, setQuery] = useState('');
    const [expanded, setExpanded] = useState<Set<string>>(DEFAULT_EXPANDED);

    const q = query.trim().toLowerCase();

    const filtered = useMemo(() => {
        if (!q) return PALETTE;
        return PALETTE.map(cat => ({
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
                    <svg
                        className="palette-search-icon"
                        width="14"
                        height="14"
                        viewBox="0 0 24 24"
                        fill="none"
                        stroke="currentColor"
                        strokeWidth="2"
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        aria-hidden="true"
                    >
                        <circle cx="11" cy="11" r="7" />
                        <line x1="21" y1="21" x2="16.65" y2="16.65" />
                    </svg>
                    <input
                        type="text"
                        className="palette-search"
                        placeholder="Search components…"
                        value={query}
                        onChange={e => setQuery(e.target.value)}
                        spellCheck={false}
                    />
                    {query ? (
                        <button
                            type="button"
                            className="palette-search-clear"
                            onClick={() => setQuery('')}
                            aria-label="Clear search"
                        >
                            ×
                        </button>
                    ) : null}
                </div>
                <div className="palette-stats">
                    <span>
                        <b>{AVAILABLE_COUNT}</b> available
                    </span>
                    <span className="palette-stats-sep">·</span>
                    <span>
                        <b>{TOTAL_COMPONENT_COUNT}</b> total
                    </span>
                </div>
            </div>

            <div className="palette-body">
                {filtered.length === 0 ? (
                    <div className="palette-empty">
                        No components match <span className="quote">{query}</span>
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
                                        {isExpanded ? '▾' : '▸'}
                                    </span>
                                    <span
                                        className="palette-cat-icon"
                                        style={{ color: cat.accent }}
                                        aria-hidden="true"
                                    >
                                        {cat.icon}
                                    </span>
                                    <span className="palette-cat-label">{cat.label}</span>
                                    <span className="palette-cat-count">{count}</span>
                                </button>
                                {isExpanded ? (
                                    <div className="palette-category-body">
                                        {cat.groups.map(g => (
                                            <div className="palette-group" key={g.id}>
                                                <div className="palette-group-label">{g.label}</div>
                                                {g.components.map(c => (
                                                    <div
                                                        key={c.id}
                                                        className={
                                                            'palette-component' +
                                                            (c.availability === 'planned'
                                                                ? ' is-planned'
                                                                : ' is-available')
                                                        }
                                                        draggable
                                                        onDragStart={e => onDragStart(e, c)}
                                                        title={c.summary ?? c.label}
                                                    >
                                                        <span
                                                            className="palette-component-dot"
                                                            style={{ background: KIND_COLOR[c.kind] }}
                                                            aria-hidden="true"
                                                        />
                                                        <span className="palette-component-label">
                                                            {c.label}
                                                        </span>
                                                        {c.availability === 'available' ? (
                                                            <span
                                                                className="palette-availability palette-availability-yes"
                                                                aria-label="available"
                                                            >
                                                                ✓
                                                            </span>
                                                        ) : (
                                                            <span
                                                                className="palette-availability palette-availability-no"
                                                                aria-label="planned"
                                                            >
                                                                ○
                                                            </span>
                                                        )}
                                                    </div>
                                                ))}
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
