import type { FileFilter } from '../../tauri-dialog';
import type { Column, NodeKind } from '../../pipeline-types';
import type { ConnectionType } from '../../canvas/connection-types';

export type FieldKind =
    | 'text'
    | 'textarea'
    | 'number'
    | 'integer'
    | 'bool'
    | 'select'
    | 'file-path'
    | 'save-path'
    | 'expression'
    | 'filter-predicate'
    | 'column'
    | 'columns'
    | 'aggregations'
    | 'casts'
    | 'key-value'
    | 'rename-columns'
    | 'connection-ref'
    | 'routine-ref'
    | 'pipeline-ref'
    | 'ducklake-snapshot';

export type SelectOption = { label: string; value: string };

/**
 * One visibility condition on another field's EFFECTIVE value (the stored
 * prop, falling back to that field's defaultValue when unset) - see
 * visibility.ts (#166 follow-up).
 */
export type FieldCondition = {
    /** Key of the controlling field/prop in the same manifest. */
    key: string;
    /** Visible when the effective value equals this (or one of these). */
    equals?: string | string[];
    /** Visible when the effective value is unset, null, or ''. */
    empty?: boolean;
};

export type Field = {
    key: string;
    label: string;
    kind: FieldKind;
    description?: string;
    required?: boolean;
    defaultValue?: unknown;
    placeholder?: string;
    options?: SelectOption[];
    filters?: FileFilter[];
    monospace?: boolean;
    rows?: number;
    /** Filter connection-ref / routine-ref dropdowns to compatible items. */
    accepts?: string[];
    /** Render a `text` field as a masked secret input (with show/hide). */
    secret?: boolean;
    /**
     * Hide this field unless every condition matches (array = AND). Absent =
     * always visible. Hidden fields keep their stored prop value - the engine
     * ignores inapplicable props, so nothing is cleared on hide.
     */
    visibleWhen?: FieldCondition | FieldCondition[];
};

export type FormSection = {
    label: string;
    fields: Field[];
    collapsible?: boolean;
    defaultCollapsed?: boolean;
};

export type SchemaSource = 'upstream' | 'declared' | 'autodetect';

export type AutodetectResult = {
    columns: Column[];
    sampleRows?: Record<string, unknown>[];
};

export type AutodetectFn = (
    props: Record<string, unknown>,
) => Promise<AutodetectResult>;

export type PortDef = {
    id: string;
    label: string;
    type: ConnectionType;
    optional?: boolean;
};

export type NodePorts = {
    inputs: PortDef[];
    outputs: PortDef[];
};

export type ComponentManifest = {
    id: string;
    kind: NodeKind;
    label: string;
    description?: string;
    sections: FormSection[];
    schemaSource: SchemaSource;
    autodetect?: AutodetectFn;
    ports?: NodePorts;
};

export type AggregationFunction =
    | 'count'
    | 'sum'
    | 'avg'
    | 'min'
    | 'max'
    | 'first'
    | 'last'
    | 'count_distinct'
    | 'approx_count_distinct'
    | 'array_agg';

export const AGG_FUNCTIONS: AggregationFunction[] = [
    'count',
    'sum',
    'avg',
    'min',
    'max',
    'first',
    'last',
    'count_distinct',
    'approx_count_distinct',
    'array_agg',
];

export type Aggregation = {
    column: string;
    func: AggregationFunction;
    output: string;
};

// #144: multi-column Cast / Convert. Each row targets one column; the engine's
// build_cast reads `casts: [{ column, targetType, format? }]`. Type values match
// the single-column Cast select so downstream schema resolution and the engine's
// duckle_type_to_duckdb map both stay in sync.
export const CAST_TYPES: SelectOption[] = [
    { label: 'string', value: 'string' },
    { label: 'int32', value: 'int32' },
    { label: 'int64', value: 'int64' },
    { label: 'float32', value: 'float32' },
    { label: 'float64', value: 'float64' },
    { label: 'bool', value: 'bool' },
    { label: 'date', value: 'date' },
    { label: 'timestamp', value: 'timestamp' },
    { label: 'decimal', value: 'decimal' },
    { label: 'json', value: 'json' },
];

export type Cast = {
    column: string;
    targetType: string;
    /** strptime format, only meaningful for date/timestamp targets. */
    format?: string;
    /** #144: per-column error handling. Unset inherits the node-level "On
     * conversion error" default. "null" nulls bad cells, "fail" aborts the run. */
    onError?: string;
};
