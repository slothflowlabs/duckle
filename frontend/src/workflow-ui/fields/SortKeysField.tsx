import { useContext } from 'react';
import { X, ChevronUp, ChevronDown } from 'lucide-react';
import { FieldContext } from './FieldContext';
import type { SortKey } from './types';

// Multi-column sort. build_sort has always preferred an `orderBy` array over
// the single `sortColumn`, and nothing in the editor could write one - the gap
// was recorded in prop_contract.rs as "multi-column sort is unreachable from
// the editor", and the Python API could express it while the GUI could not.
//
// Order matters here in a way it does not for the other list fields, so rows
// carry move-up / move-down rather than being a set of checkboxes over the
// upstream schema.

type Props = {
    value: SortKey[] | undefined;
    onChange: (v: SortKey[]) => void;
};

// Null placement is per key and tri-state on purpose. An existing `orderBy`
// emits no NULLS clause at all, so "Default" has to stay expressible - writing
// one in would silently reorder a run that never asked for it.
const NULLS_OPTIONS = [
    { label: 'Default', value: '' },
    { label: 'NULLs last', value: 'last' },
    { label: 'NULLs first', value: 'first' },
];

const GRID = { gridTemplateColumns: '1.4fr 0.9fr 0.9fr 52px 24px' };

/** The single-column form this field replaces, read so an existing node opens
 *  showing the sort it already has instead of an empty list. `nullsLast`
 *  defaults to true because that is what the engine's single-column branch has
 *  always emitted, so seeding it keeps the row order identical. */
function seedFromLegacy(props: Record<string, unknown> | undefined): SortKey[] {
    const column = typeof props?.sortColumn === 'string' ? props.sortColumn.trim() : '';
    if (!column) return [];
    return [
        {
            column,
            direction: props?.direction === 'desc' ? 'desc' : 'asc',
            nullsLast: props?.nullsLast === false ? false : true,
        },
    ];
}

export function SortKeysField({ value, onChange }: Props) {
    const { upstreamSchema, nodeProps } = useContext(FieldContext);
    const stored = value ?? [];
    // Shown, not written. The node keeps its old properties until the user
    // actually edits the sort, and the engine reads `orderBy` first, so the
    // first edit takes over cleanly with nothing to migrate.
    const seeded = stored.length === 0 ? seedFromLegacy(nodeProps) : [];
    const keys = stored.length > 0 ? stored : seeded;

    const commit = (next: SortKey[]) => onChange(next);
    const add = () => {
        const used = new Set(keys.map(k => k.column));
        const next = upstreamSchema.find(c => !used.has(c.name)) ?? upstreamSchema[0];
        commit([...keys, { column: next ? next.name : '', direction: 'asc' }]);
    };
    const update = (i: number, patch: Partial<SortKey>) =>
        commit(keys.map((k, idx) => (idx === i ? { ...k, ...patch } : k)));
    const remove = (i: number) => commit(keys.filter((_, idx) => idx !== i));
    const move = (i: number, by: number) => {
        const to = i + by;
        if (to < 0 || to >= keys.length) return;
        const next = [...keys];
        [next[i], next[to]] = [next[to], next[i]];
        commit(next);
    };

    return (
        <div className="field-aggregations">
            <div className="field-agg-toolbar">
                <span className="field-agg-count">
                    {keys.length === 0
                        ? 'no sort'
                        : `${keys.length} sort key${keys.length === 1 ? '' : 's'}`}
                </span>
                <button type="button" className="schema-add" onClick={add}>
                    + Add sort key
                </button>
            </div>
            {seeded.length > 0 ? (
                <div className="field-agg-empty">
                    Showing the single column this node was already sorting by. Add a second key
                    or change anything here to switch it to a multi-column sort.
                </div>
            ) : null}
            {keys.length === 0 ? (
                <div className="field-agg-empty">
                    Rows are returned in whatever order the source produced them. Click{' '}
                    <b>+ Add sort key</b> to order them; keys apply left to right, the first
                    breaking ties for the second.
                </div>
            ) : (
                <div className="field-agg-table">
                    <div className="field-agg-row field-agg-header" style={GRID}>
                        <div>Column</div>
                        <div>Direction</div>
                        <div>NULLs</div>
                        <div>Order</div>
                        <div />
                    </div>
                    {keys.map((k, i) => (
                        <div className="field-agg-row" key={i} style={GRID}>
                            <select
                                className="schema-input"
                                value={k.column}
                                onChange={e => update(i, { column: e.target.value })}
                            >
                                <option value="">- column -</option>
                                {upstreamSchema.map(c => (
                                    <option key={c.name} value={c.name}>
                                        {c.name}
                                    </option>
                                ))}
                                {k.column && !upstreamSchema.some(c => c.name === k.column) ? (
                                    <option value={k.column}>{k.column}  (not in input)</option>
                                ) : null}
                            </select>
                            <select
                                className="schema-input"
                                value={k.direction === 'desc' ? 'desc' : 'asc'}
                                onChange={e =>
                                    update(i, { direction: e.target.value === 'desc' ? 'desc' : 'asc' })
                                }
                            >
                                <option value="asc">Ascending</option>
                                <option value="desc">Descending</option>
                            </select>
                            <select
                                className="schema-input"
                                value={
                                    k.nullsLast === true ? 'last' : k.nullsLast === false ? 'first' : ''
                                }
                                onChange={e =>
                                    update(i, {
                                        nullsLast:
                                            e.target.value === 'last'
                                                ? true
                                                : e.target.value === 'first'
                                                  ? false
                                                  : undefined,
                                    })
                                }
                                title="Where NULLs go. Default leaves it to the database, which is what this node does today."
                            >
                                {NULLS_OPTIONS.map(o => (
                                    <option key={o.value} value={o.value}>
                                        {o.label}
                                    </option>
                                ))}
                            </select>
                            <div className="field-sort-move">
                                <button
                                    type="button"
                                    onClick={() => move(i, -1)}
                                    disabled={i === 0}
                                    aria-label={`Move ${k.column || 'key'} earlier`}
                                    title="Sort by this column sooner"
                                >
                                    <ChevronUp size={13} />
                                </button>
                                <button
                                    type="button"
                                    onClick={() => move(i, 1)}
                                    disabled={i === keys.length - 1}
                                    aria-label={`Move ${k.column || 'key'} later`}
                                    title="Sort by this column later"
                                >
                                    <ChevronDown size={13} />
                                </button>
                            </div>
                            <button
                                type="button"
                                className="schema-remove"
                                onClick={() => remove(i)}
                                aria-label={`Remove ${k.column || 'key'}`}
                            >
                                <X size={13} />
                            </button>
                        </div>
                    ))}
                </div>
            )}
        </div>
    );
}
