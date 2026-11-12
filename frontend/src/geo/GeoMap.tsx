// #372: a map of a preview's GEOMETRY column, drawn above the table it came from.
//
// Points, lines and polygons are drawn as SVG on a plain background. There is no
// basemap on purpose: tiles come from a third-party server, and opening a
// preview must not reach the network. Selection is shared with the table: a
// shape clicked here is the row highlighted there, and the other way round.

import { Fragment, memo, useCallback, useEffect, useMemo, useRef, useState, type PointerEvent as ReactPointerEvent } from 'react';
import { LocateFixed, Maximize2, Minus, Plus, X } from 'lucide-react';
import type { Position } from './wkt';
import {
    boundsOf,
    collectFeatures,
    crsOfType,
    fitView,
    geometryColumns,
    isGeographic,
    pathParts,
    toMap,
    zoomAt,
    type MapFeature,
    type View,
} from './map-view';

export const FEATURE_LIMIT_DEFAULT = 1000;
export const FEATURE_LIMIT_MAX = 10_000;
const LIMIT_KEY = 'duckle.map.featureLimit';
const PAD = 16;

function storedLimit(): number {
    try {
        const n = Number(localStorage.getItem(LIMIT_KEY));
        return Number.isInteger(n) && n >= 1 ? Math.min(n, FEATURE_LIMIT_MAX) : FEATURE_LIMIT_DEFAULT;
    } catch {
        return FEATURE_LIMIT_DEFAULT;
    }
}

function rememberLimit(n: number): void {
    try {
        localStorage.setItem(LIMIT_KEY, String(n));
    } catch {
        // A convenience for this viewer only; the map works without it.
    }
}

export type GeoMapProps = {
    columns: { name: string; type: string }[];
    rows: Record<string, unknown>[];
    /** The DuckDB type of a column, when the run reported it; it carries the CRS. */
    sqlTypeOf?: (column: string) => string | undefined;
    selectedRow: number | null;
    onSelectRow: (row: number | null) => void;
    /** Rows the step produced in all, when known, so the map can say what it holds. */
    totalRows?: number;
    /** Run to this step again keeping `rows` preview rows. Absent where there is no step to run. */
    onLoadRows?: (rows: number) => void;
    loading?: boolean;
    height?: number;
};

function formatValue(v: unknown): string {
    if (v === null || v === undefined) return '∅';
    if (typeof v === 'object') return JSON.stringify(v);
    return String(v);
}

function plural(n: number, one: string, many = one + 's'): string {
    return `${n.toLocaleString()} ${n === 1 ? one : many}`;
}

/** One feature, its row number on the group so a click can find it. */
const Shape = memo(function Shape({
    feature,
    origin,
    selected,
}: {
    feature: MapFeature;
    origin: Position;
    selected?: boolean;
}) {
    const p = pathParts(feature.geometry, origin[0], origin[1]);
    return (
        <g data-row={feature.row} className={'geo-shape' + (selected ? ' is-selected' : '')}>
            {p.polygons ? <path className="geo-area" d={p.polygons} /> : null}
            {p.lines ? <path className="geo-line" d={p.lines} /> : null}
            {p.lines ? <path className="geo-hit" d={p.lines} /> : null}
            {p.points ? <path className="geo-point" d={p.points} /> : null}
        </g>
    );
});

export function GeoMap({
    columns,
    rows,
    sqlTypeOf,
    selectedRow,
    onSelectRow,
    totalRows,
    onLoadRows,
    loading,
    height = 260,
}: GeoMapProps) {
    const geoCols = useMemo(() => geometryColumns(columns), [columns]);
    const [chosen, setChosen] = useState<string | null>(null);
    const column = chosen && geoCols.includes(chosen) ? chosen : geoCols[0];

    const [limit, setLimit] = useState(storedLimit);
    const [draft, setDraft] = useState(() => String(limit));
    const commitLimit = () => {
        const n = Math.min(Math.max(Math.round(Number(draft)) || 1, 1), FEATURE_LIMIT_MAX);
        setLimit(n);
        setDraft(String(n));
        rememberLimit(n);
    };

    const set = useMemo(() => collectFeatures(rows, column ?? '', limit), [rows, column, limit]);
    const bounds = useMemo(() => boundsOf(set.features), [set]);
    const origin = useMemo<Position | null>(
        () => (bounds ? [(bounds.minX + bounds.maxX) / 2, (bounds.minY + bounds.maxY) / 2] : null),
        [bounds],
    );
    const crs = column ? crsOfType(sqlTypeOf?.(column)) : null;
    const geographic = bounds ? isGeographic(crs, bounds) : false;

    const boxRef = useRef<HTMLDivElement>(null);
    const svgRef = useRef<SVGSVGElement>(null);
    const [width, setWidth] = useState(0);
    useEffect(() => {
        const el = boxRef.current;
        if (!el) return;
        setWidth(el.clientWidth);
        const ro = new ResizeObserver(() => setWidth(el.clientWidth));
        ro.observe(el);
        return () => ro.disconnect();
    }, []);

    const [view, setView] = useState<View | null>(null);
    const fit = useCallback(() => {
        if (bounds && width > 0) setView(fitView(bounds, width, height, PAD, geographic));
    }, [bounds, width, height, geographic]);
    // Fit once per set of shapes; a later resize keeps whatever the user zoomed to.
    const fittedFor = useRef<typeof set | null>(null);
    useEffect(() => {
        if (!bounds || width <= 0 || (fittedFor.current === set && view)) return;
        fittedFor.current = set;
        setView(fitView(bounds, width, height, PAD, geographic));
    }, [set, bounds, width, height, geographic, view]);

    const hasShapes = set.features.length > 0;
    // React's onWheel is passive, so it cannot stop the panel scrolling under a zoom.
    useEffect(() => {
        const el = svgRef.current;
        if (!el) return;
        const onWheel = (e: WheelEvent) => {
            e.preventDefault();
            const r = el.getBoundingClientRect();
            const delta = e.deltaMode === 1 ? e.deltaY * 16 : e.deltaY;
            const factor = Math.min(Math.max(Math.exp(-delta * 0.0015), 0.5), 2);
            setView(v => (v ? zoomAt(v, factor, e.clientX - r.left, e.clientY - r.top) : v));
        };
        el.addEventListener('wheel', onWheel, { passive: false });
        return () => el.removeEventListener('wheel', onWheel);
    }, [hasShapes]);

    // A press that moves under 3px is a click; the shape is taken at the press,
    // because pointer capture makes the svg the target of the release.
    const drag = useRef<{ x: number; y: number; view: View; moved: boolean; row: number | null } | null>(null);
    const [dragging, setDragging] = useState(false);
    const [cursor, setCursor] = useState<Position | null>(null);
    const onPointerDown = (e: ReactPointerEvent<SVGSVGElement>) => {
        if (e.button !== 0 || !view) return;
        const hit = (e.target as Element).closest('[data-row]');
        drag.current = {
            x: e.clientX,
            y: e.clientY,
            view,
            moved: false,
            row: hit ? Number(hit.getAttribute('data-row')) : null,
        };
        e.currentTarget.setPointerCapture(e.pointerId);
    };
    const onPointerMove = (e: ReactPointerEvent<SVGSVGElement>) => {
        const d = drag.current;
        if (d) {
            const dx = e.clientX - d.x;
            const dy = e.clientY - d.y;
            if (!d.moved && Math.hypot(dx, dy) < 3) return;
            if (!d.moved) setDragging(true);
            d.moved = true;
            setView({ ...d.view, tx: d.view.tx + dx, ty: d.view.ty + dy });
        } else if (view) {
            const r = e.currentTarget.getBoundingClientRect();
            setCursor(toMap(view, [e.clientX - r.left, e.clientY - r.top]));
        }
    };
    const onPointerUp = () => {
        const d = drag.current;
        drag.current = null;
        setDragging(false);
        if (d && !d.moved) onSelectRow(d.row);
    };
    const zoomCentre = (factor: number) => setView(v => (v && width > 0 ? zoomAt(v, factor, width / 2, height / 2) : v));

    const selectedFeature = selectedRow === null ? undefined : set.features.find(f => f.row === selectedRow);
    const zoomToSelected = () => {
        if (!selectedFeature) return;
        const b = boundsOf([selectedFeature]);
        if (b && width > 0) setView(fitView(b, width, height, PAD * 3, geographic));
    };

    const layer = useMemo(
        () => (origin ? set.features.map(f => <Shape key={f.row} feature={f} origin={origin} />) : null),
        [set, origin],
    );

    if (!column) return null;
    const transform =
        view && origin
            ? `matrix(${view.scale * view.xStretch} 0 0 ${-view.scale} ${view.tx + origin[0] * view.scale * view.xStretch} ${view.ty - origin[1] * view.scale})`
            : undefined;
    const digits = geographic ? 5 : 2;
    const held = rows.length;
    const canLoad = !!onLoadRows && totalRows !== undefined && totalRows > held && limit > held;
    const loadCount = totalRows !== undefined ? Math.min(limit, totalRows) : limit;
    const left: string[] = [];
    if (set.nulls) left.push(`${set.nulls.toLocaleString()} null`);
    if (set.empty) left.push(`${set.empty.toLocaleString()} empty`);
    if (set.unreadable) left.push(`${set.unreadable.toLocaleString()} not readable as WKT (masked?)`);
    if (set.capped) left.push(`${plural(set.capped, 'row')} over the feature limit`);
    const row = selectedRow === null ? undefined : rows[selectedRow];

    return (
        <div className="geo-map">
            <div className="geo-bar">
                {geoCols.length > 1 ? (
                    <select className="geo-col-select" value={column} onChange={e => setChosen(e.target.value)} title="Geometry column to draw">
                        {geoCols.map(c => (
                            <option key={c} value={c}>
                                {c}
                            </option>
                        ))}
                    </select>
                ) : (
                    <span className="geo-col">{column}</span>
                )}
                <span className="geo-crs" title="The coordinate reference system the column's type names">
                    {crs ?? (geographic ? 'CRS unknown, drawn as longitude/latitude' : 'CRS unknown')}
                </span>
                <span className="geo-spacer" />
                <label className="geo-limit" title="Most shapes to draw, so a large preview stays responsive">
                    Feature limit
                    <input
                        type="number"
                        min={1}
                        max={FEATURE_LIMIT_MAX}
                        step={100}
                        value={draft}
                        onChange={e => setDraft(e.target.value)}
                        onBlur={commitLimit}
                        onKeyDown={e => {
                            if (e.key === 'Enter') commitLimit();
                        }}
                    />
                </label>
                {canLoad ? (
                    <button
                        type="button"
                        className="btn btn-sm geo-load"
                        disabled={loading}
                        onClick={() => onLoadRows?.(loadCount)}
                        title="Run to this step again, keeping this many of its rows for the map"
                    >
                        {loading ? 'Running…' : `Load ${loadCount.toLocaleString()} rows`}
                    </button>
                ) : null}
                <button type="button" className="geo-icon-btn" onClick={() => zoomCentre(1.5)} title="Zoom in" aria-label="Zoom in">
                    <Plus size={14} />
                </button>
                <button type="button" className="geo-icon-btn" onClick={() => zoomCentre(1 / 1.5)} title="Zoom out" aria-label="Zoom out">
                    <Minus size={14} />
                </button>
                <button type="button" className="geo-icon-btn" onClick={fit} title="Fit to the shapes" aria-label="Fit to the shapes">
                    <Maximize2 size={14} />
                </button>
            </div>
            <div className={'geo-canvas' + (dragging ? ' is-dragging' : '')} ref={boxRef} style={{ height }}>
                {hasShapes ? (
                    <svg
                        ref={svgRef}
                        width={width}
                        height={height}
                        onPointerDown={onPointerDown}
                        onPointerMove={onPointerMove}
                        onPointerUp={onPointerUp}
                        onPointerCancel={onPointerUp}
                        onPointerLeave={() => setCursor(null)}
                    >
                        {transform ? (
                            <g transform={transform}>
                                {layer}
                                {selectedFeature && origin ? <Shape feature={selectedFeature} origin={origin} selected /> : null}
                            </g>
                        ) : null}
                    </svg>
                ) : (
                    <div className="geo-empty">No shapes to draw in these rows.</div>
                )}
                {row ? (
                    <div className="geo-card">
                        <div className="geo-card-head">
                            <span>Row {(selectedRow ?? 0) + 1}</span>
                            <span className="geo-spacer" />
                            {selectedFeature ? (
                                <button type="button" className="geo-icon-btn" onClick={zoomToSelected} title="Zoom to this shape" aria-label="Zoom to this shape">
                                    <LocateFixed size={13} />
                                </button>
                            ) : null}
                            <button type="button" className="geo-icon-btn" onClick={() => onSelectRow(null)} title="Close" aria-label="Close">
                                <X size={13} />
                            </button>
                        </div>
                        <dl>
                            {columns
                                .filter(c => c.name !== column)
                                .map(c => (
                                    <Fragment key={c.name}>
                                        <dt>{c.name}</dt>
                                        <dd>{formatValue(row[c.name])}</dd>
                                    </Fragment>
                                ))}
                        </dl>
                    </div>
                ) : null}
                {cursor ? (
                    <div className="geo-cursor">
                        {cursor[0].toFixed(digits)}, {cursor[1].toFixed(digits)}
                    </div>
                ) : null}
            </div>
            <div className="geo-status">
                {plural(set.features.length, 'shape')} drawn
                {left.length ? ` · ${left.join(' · ')}` : ''}
                {totalRows !== undefined && totalRows > held
                    ? ` · this preview holds ${held.toLocaleString()} of ${plural(totalRows, 'row')}`
                    : ''}
            </div>
        </div>
    );
}
