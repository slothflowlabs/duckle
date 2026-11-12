// Opens a dive: runs its query (never-stale) and renders the chart plus the
// underlying-data table. If the chart's encoding references a column the query
// did not return, fall back to the table only rather than rendering a broken
// chart. See docs/design/dives.md.

import { useEffect, useRef, useState } from 'react';
import { Map as MapIcon } from 'lucide-react';
import type { Dive } from './dive-types';
import { runDive, type DiveResult } from './dive-run';
import { VegaChart } from './VegaChart';
import { suggestChart } from './suggest-chart';
import { GeoMap } from '../geo/GeoMap';
import { geometryColumns } from '../geo/map-view';

interface DivePanelProps {
    dive: Dive;
    workspacePath?: string | null;
    theme?: 'light' | 'dark';
}

/** Field names referenced by a Vega-Lite spec's encoding channels. */
function encodingFields(spec: Record<string, unknown>): string[] {
    const enc = (spec.encoding ?? {}) as Record<string, unknown>;
    const fields: string[] = [];
    for (const channel of Object.values(enc)) {
        const f = (channel as Record<string, unknown> | null)?.field;
        if (typeof f === 'string') fields.push(f);
    }
    return fields;
}

export function DivePanel({ dive, workspacePath, theme = 'dark' }: DivePanelProps) {
    const [result, setResult] = useState<DiveResult | null>(null);
    const [error, setError] = useState<string | null>(null);
    const [loading, setLoading] = useState(true);
    // #372: a dive whose query returns GEOMETRY can be drawn on a map.
    const [showMap, setShowMap] = useState(false);
    const [selectedRow, setSelectedRow] = useState<number | null>(null);

    useEffect(() => {
        let cancelled = false;
        setLoading(true);
        setError(null);
        runDive(dive, workspacePath)
            .then((r) => {
                if (!cancelled) {
                    setResult(r);
                    setSelectedRow(null);
                    setLoading(false);
                }
            })
            .catch((e: unknown) => {
                if (!cancelled) {
                    setError(e instanceof Error ? e.message : String(e));
                    setLoading(false);
                }
            });
        return () => {
            cancelled = true;
        };
    }, [dive, workspacePath]);

    if (loading) return <div className="dive-panel-msg">Running query...</div>;
    if (error) return <div className="dive-panel-msg dive-panel-err">{error}</div>;
    if (!result) return null;

    const cols = result.columns.map((c) => c.name);
    const declared = encodingFields(dive.chart);
    const declaredOk = declared.length > 0 && declared.every((f) => cols.includes(f));
    // Use the dive's own chart when its fields resolve against the result, else
    // auto-pick one from the result shape; null means nothing sensible to chart.
    const spec = declaredOk ? dive.chart : suggestChart(result.columns);
    const chartRenderable = spec !== null && encodingFields(spec).every((f) => cols.includes(f));
    const mappable = geometryColumns(result.columns).length > 0;
    const sqlTypeOf = (column: string) => {
        const at = result.columns.findIndex((c) => c.name === column);
        return at >= 0 ? result.sqlTypes?.[at] : undefined;
    };

    return (
        <div className="dive-panel">
            <div className="dive-panel-head">
                {dive.title}
                {mappable ? (
                    <button
                        type="button"
                        className={'preview-map-toggle' + (showMap ? ' is-on' : '')}
                        onClick={() => setShowMap((on) => !on)}
                        title="Draw this dive's geometry on a map"
                    >
                        <MapIcon size={11} /> {showMap ? 'Hide map' : 'View on map'}
                    </button>
                ) : null}
            </div>
            {chartRenderable && spec ? (
                <VegaChart spec={spec} rows={result.rows} theme={theme} />
            ) : null}
            {mappable && showMap ? (
                <GeoMap
                    columns={result.columns}
                    rows={result.rows}
                    sqlTypeOf={sqlTypeOf}
                    selectedRow={selectedRow}
                    onSelectRow={setSelectedRow}
                />
            ) : null}
            <DiveTable
                columns={cols}
                rows={result.rows}
                selectedRow={showMap ? selectedRow : null}
                onSelectRow={showMap ? setSelectedRow : undefined}
            />
        </div>
    );
}

function fmt(v: unknown): string {
    if (v === null || v === undefined) return '';
    if (typeof v === 'object') return JSON.stringify(v);
    return String(v);
}

function DiveTable({
    columns,
    rows,
    selectedRow,
    onSelectRow,
}: {
    columns: string[];
    rows: Record<string, unknown>[];
    selectedRow?: number | null;
    onSelectRow?: (row: number) => void;
}) {
    const shown = rows.slice(0, 100);
    const tbodyRef = useRef<HTMLTableSectionElement>(null);
    useEffect(() => {
        if (selectedRow !== null && selectedRow !== undefined) {
            tbodyRef.current?.children[selectedRow]?.scrollIntoView({ block: 'nearest' });
        }
    }, [selectedRow]);
    return (
        <div className="dive-table-wrap">
            <table className="dive-table">
                <thead>
                    <tr>
                        {columns.map((c) => (
                            <th key={c}>{c}</th>
                        ))}
                    </tr>
                </thead>
                <tbody ref={tbodyRef}>
                    {shown.map((row, i) => (
                        <tr
                            key={i}
                            className={i === selectedRow ? 'is-selected' : undefined}
                            onClick={onSelectRow ? () => onSelectRow(i) : undefined}
                        >
                            {columns.map((c) => (
                                <td key={c}>{fmt(row[c])}</td>
                            ))}
                        </tr>
                    ))}
                </tbody>
            </table>
            {rows.length > shown.length ? (
                <div className="dive-table-more">{rows.length - shown.length} more rows</div>
            ) : null}
        </div>
    );
}
