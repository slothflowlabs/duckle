export type DataType =
    | 'string'
    | 'int32'
    | 'int64'
    | 'float32'
    | 'float64'
    | 'bool'
    | 'date'
    | 'timestamp'
    | 'time'
    | 'decimal'
    | 'json'
    | 'binary'
    | 'geometry';

export const DATA_TYPES: DataType[] = [
    'string',
    'int32',
    'int64',
    'float32',
    'float64',
    'bool',
    'date',
    'timestamp',
    'time',
    'decimal',
    'json',
    'binary',
    'geometry',
];

export type Column = {
    name: string;
    type: DataType;
    nullable: boolean;
    primaryKey?: boolean;
    // Optional strptime format (e.g. %d/%m/%Y) for date/timestamp columns,
    // so several date columns can each parse a different layout on one read.
    format?: string;
};

export type NodeKind = 'source' | 'transform' | 'sink';

/**
 * A note on the canvas, as Talend, Hop and NiFi have them: text and nothing else.
 * Notes live beside a pipeline's nodes, not among them, so nothing that runs,
 * validates or compiles a pipeline ever sees one.
 */
export type NoteData = { text: string };

export type DuckleNodeData = {
    label: string;
    subtitle?: string;
    componentId?: string;
    // Optional user-defined SQL name for this node's output relation. When set,
    // raw / pure SQL nodes can reference this node by a friendly name instead of
    // the auto-generated node id (#102). Must be unique across the pipeline.
    alias?: string;
    properties?: Record<string, unknown>;
    schema?: Column[];
    sampleRows?: Record<string, unknown>[];
    [key: string]: unknown;
};
