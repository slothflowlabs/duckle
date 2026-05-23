import { PALETTE, type ComponentDef } from '../palette-data';
import type {
    ComponentManifest,
    Field,
    FormSection,
    NodePorts,
    SchemaSource,
    AutodetectResult,
} from './types';

type PaletteEntry = { categoryId: string; groupId: string; comp: ComponentDef };

function findPaletteEntry(componentId: string): PaletteEntry | null {
    for (const cat of PALETTE) {
        for (const grp of cat.groups) {
            for (const comp of grp.components) {
                if (comp.id === componentId) {
                    return { categoryId: cat.id, groupId: grp.id, comp };
                }
            }
        }
    }
    return null;
}

import { tauriAutodetect } from '../../tauri-bridge';

function placeholderAutodetect(format?: string): (
    props: Record<string, unknown>,
) => Promise<AutodetectResult> {
    return async (props: Record<string, unknown>) => {
        // If we know the format and a path is set, try the real Rust
        // autodetect command via Tauri; fall back to a placeholder.
        if (format) {
            const path = typeof props.path === 'string' ? props.path.trim() : '';
            if (path) {
                const real = await tauriAutodetect(format, props);
                if (real) return { columns: real.columns, sampleRows: real.sampleRows };
            }
        }
        await new Promise(r => setTimeout(r, 250));
        return {
            columns: [
                { name: 'col_1', type: 'string', nullable: true },
                { name: 'col_2', type: 'int64', nullable: true },
                { name: 'col_3', type: 'timestamp', nullable: true },
            ],
            sampleRows: [],
        };
    };
}

// Field helpers ---------------------------------------------------------

const encodingField = (): Field => ({
    key: 'encoding',
    label: 'Encoding',
    kind: 'select',
    defaultValue: 'utf-8',
    options: [
        { label: 'UTF-8', value: 'utf-8' },
        { label: 'UTF-16', value: 'utf-16' },
        { label: 'Latin-1', value: 'latin-1' },
        { label: 'Windows-1252', value: 'windows-1252' },
    ],
});

const writeModeField = (): Field => ({
    key: 'mode',
    label: 'Write mode',
    kind: 'select',
    defaultValue: 'overwrite',
    options: [
        { label: 'Overwrite', value: 'overwrite' },
        { label: 'Error if exists', value: 'error' },
    ],
});

const compressionField = (): Field => ({
    key: 'compression',
    label: 'Compression',
    kind: 'select',
    defaultValue: 'none',
    options: [
        { label: 'None', value: 'none' },
        { label: 'Gzip', value: 'gzip' },
        { label: 'Zstd', value: 'zstd' },
        { label: 'Snappy', value: 'snappy' },
    ],
});

const credentialFields = (): Field[] => [
    { key: 'username', label: 'Username', kind: 'text' },
    { key: 'password', label: 'Password', kind: 'text', placeholder: '••••••••' },
    {
        key: 'connectionRef',
        label: 'Or use saved connection',
        kind: 'connection-ref',
        description: 'Pick a connection from the Connections folder.',
    },
];

const DB_PORTS: Record<string, number> = {
    'src.postgres': 5432,
    'snk.postgres': 5432,
    'src.mysql': 3306,
    'snk.mysql': 3306,
    'src.mariadb': 3306,
    'snk.mariadb': 3306,
    'src.sqlserver': 1433,
    'snk.sqlserver': 1433,
    'src.oracle': 1521,
    'snk.oracle': 1521,
    'src.db2': 50000,
    'src.clickhouse': 8123,
    'snk.clickhouse': 8123,
    'src.cockroach': 26257,
    'src.mongodb': 27017,
    'snk.mongodb': 27017,
    'src.cassandra': 9042,
    'src.scylla': 9042,
    'src.redis': 6379,
    'snk.redis': 6379,
    'src.elastic': 9200,
    'snk.elastic': 9200,
    'src.opensearch': 9200,
    'snk.opensearch': 9200,
    'src.couchdb': 5984,
    'src.kafka': 9092,
    'snk.kafka': 9092,
    'src.pulsar': 6650,
    'snk.pulsar': 6650,
    'src.nats': 4222,
    'snk.nats': 4222,
    'src.rabbit': 5672,
};

const dbConnectionFields = (componentId: string): Field[] => [
    { key: 'host', label: 'Host', kind: 'text', required: true, placeholder: 'localhost' },
    {
        key: 'port',
        label: 'Port',
        kind: 'integer',
        defaultValue: DB_PORTS[componentId] ?? 0,
    },
    { key: 'database', label: 'Database', kind: 'text', required: true, placeholder: 'mydb' },
    ...credentialFields(),
];

const dbReadFields = (): Field[] => [
    {
        key: 'mode',
        label: 'Read mode',
        kind: 'select',
        defaultValue: 'table',
        options: [
            { label: 'Whole table', value: 'table' },
            { label: 'Custom SQL', value: 'sql' },
            { label: 'Incremental (by column)', value: 'incremental' },
        ],
    },
    { key: 'schemaName', label: 'Schema', kind: 'text', placeholder: 'public' },
    { key: 'tableName', label: 'Table', kind: 'text', placeholder: 'orders' },
    {
        key: 'sql',
        label: 'SQL query',
        kind: 'expression',
        rows: 5,
        placeholder: 'SELECT * FROM orders WHERE status = $1',
    },
    {
        key: 'incrementalColumn',
        label: 'Incremental column',
        kind: 'text',
        placeholder: 'updated_at',
    },
    {
        key: 'fetchSize',
        label: 'Fetch size',
        kind: 'integer',
        defaultValue: 1000,
        description: 'Rows fetched per round-trip.',
    },
];

const dbWriteFields = (): Field[] => [
    { key: 'schemaName', label: 'Schema', kind: 'text', placeholder: 'public' },
    { key: 'tableName', label: 'Table', kind: 'text', required: true, placeholder: 'orders' },
    {
        key: 'mode',
        label: 'Write mode',
        kind: 'select',
        defaultValue: 'overwrite',
        options: [
            { label: 'Create or replace', value: 'overwrite' },
            { label: 'Append (insert)', value: 'append' },
            { label: 'Upsert (insert on conflict / on duplicate key)', value: 'upsert' },
            { label: 'Truncate + insert', value: 'truncate' },
        ],
    },
    {
        key: 'conflictColumns',
        label: 'Conflict columns',
        kind: 'columns',
        description: 'Used in upsert mode: Postgres / Cockroach use these as ON CONFLICT keys; MySQL / MariaDB rely on the target table\'s existing UNIQUE / PRIMARY KEY index.',
    },
];

// Synthesizers ---------------------------------------------------------

function base(
    comp: ComponentDef,
    sections: FormSection[],
    schemaSource: SchemaSource = 'autodetect',
): ComponentManifest {
    const kind: ComponentManifest['kind'] =
        comp.kind === 'source' || comp.kind === 'sink' ? comp.kind : 'transform';
    return {
        id: comp.id,
        kind,
        label: comp.label,
        description: comp.summary ?? defaultDescription(comp),
        schemaSource,
        autodetect:
            schemaSource === 'autodetect'
                ? placeholderAutodetect(formatFromComponent(comp.id))
                : undefined,
        sections,
        ports: portsForComponent(comp),
    };
}

/// Map a component id to the autodetect format the runtime understands.
function formatFromComponent(componentId: string): string | undefined {
    const part = componentId.split('.')[1];
    if (!part) return undefined;
    // src.csv -> csv, snk.parquet -> parquet, etc.
    return part;
}

// Port topology per component ----------------------------------------------

const MAIN_IN: NodePorts['inputs'][number] = { id: 'main', label: 'main', type: 'main' };
const MAIN_OUT: NodePorts['outputs'][number] = { id: 'main', label: 'main', type: 'main' };
const REJECT_OUT: NodePorts['outputs'][number] = {
    id: 'reject',
    label: 'reject',
    type: 'reject',
    optional: true,
};
const REJECT_IN: NodePorts['inputs'][number] = {
    id: 'reject',
    label: 'reject',
    type: 'reject',
    optional: true,
};

export function portsForComponent(comp: ComponentDef): NodePorts {
    const id = comp.id;

    // Mapper - 1 main input, up to 3 lookup inputs, main + reject outputs
    if (id === 'xf.map') {
        return {
            inputs: [
                MAIN_IN,
                { id: 'lookup_1', label: 'lookup_1', type: 'lookup', optional: true },
                { id: 'lookup_2', label: 'lookup_2', type: 'lookup', optional: true },
                { id: 'lookup_3', label: 'lookup_3', type: 'lookup', optional: true },
            ],
            outputs: [MAIN_OUT, REJECT_OUT],
        };
    }

    // Filter rows: main input, pass output, filtered output, reject output
    if (id === 'xf.filter') {
        return {
            inputs: [MAIN_IN],
            outputs: [
                { id: 'main', label: 'pass', type: 'main' },
                { id: 'filter', label: 'reject', type: 'filter' },
                { id: 'reject', label: 'errors', type: 'reject', optional: true },
            ],
        };
    }

    // Joins: driving + lookup inputs, matched + unmatched outputs
    if (id.startsWith('xf.join.') || id === 'xf.lookup' || id === 'xf.semi' || id === 'xf.anti') {
        return {
            inputs: [
                { id: 'main', label: 'driving', type: 'main' },
                { id: 'lookup', label: 'lookup', type: 'lookup' },
            ],
            outputs: [
                { id: 'main', label: 'matched', type: 'main' },
                { id: 'reject', label: 'unmatched', type: 'reject', optional: true },
            ],
        };
    }

    // Replicate - one in, multiple outs
    if (id === 'ctl.replicate') {
        return {
            inputs: [MAIN_IN],
            outputs: [
                { id: 'main_1', label: 'main 1', type: 'main' },
                { id: 'main_2', label: 'main 2', type: 'main' },
                { id: 'main_3', label: 'main 3', type: 'main', optional: true },
            ],
        };
    }

    // Merge streams - multiple inputs concatenated into one output
    if (id === 'ctl.merge') {
        return {
            inputs: [
                { id: 'main_1', label: 'left', type: 'main' },
                { id: 'main_2', label: 'right', type: 'main' },
                { id: 'main_3', label: 'extra', type: 'main', optional: true },
            ],
            outputs: [MAIN_OUT],
        };
    }

    // Switch - one in, conditional outs + else
    if (id === 'ctl.switch') {
        return {
            inputs: [MAIN_IN],
            outputs: [
                { id: 'case_1', label: 'case 1', type: 'main' },
                { id: 'case_2', label: 'case 2', type: 'main' },
                { id: 'case_3', label: 'case 3', type: 'main', optional: true },
                { id: 'default', label: 'else', type: 'main' },
            ],
        };
    }

    // Set operations - multiple inputs, one output
    if (id === 'xf.union' || id === 'xf.unionall' || id === 'xf.intersect' || id === 'xf.except') {
        return {
            inputs: [
                { id: 'main_1', label: 'left', type: 'main' },
                { id: 'main_2', label: 'right', type: 'main' },
                { id: 'main_3', label: 'extra', type: 'main', optional: true },
            ],
            outputs: [MAIN_OUT],
        };
    }

    // Iterate / foreach - emits per-row iteration
    if (id === 'ctl.iterate' || id === 'ctl.foreach') {
        return {
            inputs: [MAIN_IN],
            outputs: [{ id: 'iterate', label: 'iterate', type: 'iterate' }],
        };
    }

    // Quality validators - pass + reject
    if (comp.kind === 'quality') {
        return {
            inputs: [MAIN_IN],
            outputs: [
                { id: 'main', label: 'pass', type: 'main' },
                { id: 'reject', label: 'reject', type: 'reject' },
            ],
        };
    }

    // CDC components - changed rows out + reject + optional unchanged
    if (id.startsWith('xf.cdc.')) {
        return {
            inputs: [
                { id: 'main', label: 'new', type: 'main' },
                { id: 'lookup', label: 'previous', type: 'lookup' },
            ],
            outputs: [
                { id: 'main', label: 'changed', type: 'main' },
                { id: 'filter', label: 'unchanged', type: 'filter', optional: true },
                REJECT_OUT,
            ],
        };
    }

    // Sources: outputs only
    if (comp.kind === 'source') {
        return {
            inputs: [],
            outputs: [MAIN_OUT, REJECT_OUT],
        };
    }

    // Sinks: inputs only
    if (comp.kind === 'sink') {
        return {
            inputs: [MAIN_IN, REJECT_IN],
            outputs: [],
        };
    }

    // Default transform / control / quality / custom: main in, main out, optional reject
    return {
        inputs: [MAIN_IN],
        outputs: [MAIN_OUT, REJECT_OUT],
    };
}

function defaultDescription(comp: ComponentDef): string {
    const kindLabel =
        comp.kind === 'source'
            ? 'Reads data from'
            : comp.kind === 'sink'
              ? 'Writes data to'
              : comp.kind === 'control'
                ? 'Controls flow with'
                : comp.kind === 'quality'
                  ? 'Validates / profiles data with'
                  : comp.kind === 'custom'
                    ? 'Runs custom code via'
                    : 'Transforms data with';
    return `${kindLabel} ${comp.label}.`;
}

function synthFileSource(comp: ComponentDef): ComponentManifest {
    const ext = comp.id.split('.').pop() ?? 'txt';
    return base(comp, [
        {
            label: 'Source file',
            fields: [
                {
                    key: 'path',
                    label: 'Path',
                    kind: 'file-path',
                    required: true,
                    filters: [
                        { name: comp.label, extensions: [ext] },
                        { name: 'All files', extensions: ['*'] },
                    ],
                },
                encodingField(),
                {
                    key: 'glob',
                    label: 'Glob pattern',
                    kind: 'bool',
                    defaultValue: false,
                    description: 'Treat the path as a glob (e.g. data/*.csv) to read many files.',
                },
            ],
        },
        ...fileFormatSection(comp),
    ]);
}

function synthFileSink(comp: ComponentDef): ComponentManifest {
    const ext = comp.id.split('.').pop() ?? 'txt';
    return base(
        comp,
        [
            {
                label: 'Destination file',
                fields: [
                    {
                        key: 'path',
                        label: 'Output path',
                        kind: 'save-path',
                        required: true,
                        filters: [
                            { name: comp.label, extensions: [ext] },
                            { name: 'All files', extensions: ['*'] },
                        ],
                    },
                    writeModeField(),
                    encodingField(),
                    compressionField(),
                ],
            },
            ...fileFormatSection(comp),
        ],
        'upstream',
    );
}

function fileFormatSection(comp: ComponentDef): FormSection[] {
    const id = comp.id;
    if (id.endsWith('.csv') || id.endsWith('.tsv')) {
        return [
            {
                label: 'Format',
                fields: [
                    { key: 'hasHeader', label: 'Has header row', kind: 'bool', defaultValue: true },
                    {
                        key: 'delimiter',
                        label: 'Delimiter',
                        kind: 'select',
                        defaultValue: id.endsWith('.tsv') ? '\t' : ',',
                        options: [
                            { label: 'Comma  ,', value: ',' },
                            { label: 'Tab  \\t', value: '\t' },
                            { label: 'Semicolon  ;', value: ';' },
                            { label: 'Pipe  |', value: '|' },
                            { label: 'Space', value: ' ' },
                        ],
                    },
                    {
                        key: 'quoteChar',
                        label: 'Quote character',
                        kind: 'select',
                        defaultValue: '"',
                        options: [
                            { label: 'Double quote  "', value: '"' },
                            { label: "Single quote  '", value: "'" },
                            { label: 'None', value: '' },
                        ],
                    },
                    { key: 'skipLines', label: 'Skip lines', kind: 'integer', defaultValue: 0 },
                ],
            },
        ];
    }
    if (id.endsWith('.json') || id.endsWith('.jsonl')) {
        return [
            {
                label: 'Format',
                fields: [
                    {
                        key: 'format',
                        label: 'JSON format',
                        kind: 'select',
                        defaultValue: 'auto',
                        options: [
                            { label: 'Auto-detect', value: 'auto' },
                            { label: 'JSON array', value: 'array' },
                            { label: 'JSON Lines', value: 'jsonl' },
                            { label: 'Single object', value: 'object' },
                        ],
                    },
                    { key: 'flatten', label: 'Flatten nested objects', kind: 'bool', defaultValue: false },
                ],
            },
        ];
    }
    if (id.endsWith('.excel')) {
        return [
            {
                label: 'Format',
                fields: [
                    { key: 'sheet', label: 'Sheet name', kind: 'text', placeholder: 'Sheet1' },
                    { key: 'range', label: 'Cell range', kind: 'text', placeholder: 'A1:F1000' },
                ],
            },
        ];
    }
    if (id.endsWith('.xml')) {
        return [
            {
                label: 'Format',
                fields: [
                    { key: 'rootPath', label: 'Root element XPath', kind: 'text', placeholder: '/root/record' },
                    { key: 'namespace', label: 'XML namespace', kind: 'text' },
                ],
            },
        ];
    }
    if (id.endsWith('.parquet')) {
        return [
            {
                label: 'Format',
                fields: [
                    {
                        key: 'rowGroupSize',
                        label: 'Row group size',
                        kind: 'integer',
                        defaultValue: 100000,
                    },
                    {
                        key: 'columns',
                        label: 'Projection',
                        kind: 'text',
                        placeholder: 'leave blank for all',
                        description: 'Comma-separated column projection (read-side only).',
                    },
                ],
            },
        ];
    }
    if (id.endsWith('.fixedwidth')) {
        return [
            {
                label: 'Format',
                fields: [
                    {
                        key: 'columnWidths',
                        label: 'Column widths',
                        kind: 'text',
                        placeholder: '10,20,8,30',
                        description: 'Comma-separated character widths per column.',
                    },
                ],
            },
        ];
    }
    return [];
}

function synthLakehouseSource(comp: ComponentDef): ComponentManifest {
    // Iceberg + Delta both take a path to the table location: a local
    // directory containing the metadata + data files, or an `s3://...`
    // URL backed by a cloud SECRET configured under Connections.
    return base(comp, [
        {
            label: 'Table',
            fields: [
                {
                    key: 'path',
                    label: 'Table path',
                    kind: 'text',
                    required: true,
                    placeholder: 's3://lake/orders/  or  /var/lakes/orders',
                    description: 'The Iceberg / Delta table root: a local directory or an s3:// URL.',
                },
            ],
        },
    ]);
}

function synthDbSource(comp: ComponentDef): ComponentManifest {
    return base(comp, [
        { label: 'Connection', fields: dbConnectionFields(comp.id) },
        { label: 'Query', fields: dbReadFields() },
    ]);
}

function synthDbSink(comp: ComponentDef): ComponentManifest {
    return base(
        comp,
        [
            { label: 'Connection', fields: dbConnectionFields(comp.id) },
            { label: 'Destination', fields: dbWriteFields() },
        ],
        'upstream',
    );
}

function synthWarehouseSource(comp: ComponentDef): ComponentManifest {
    if (comp.id === 'src.motherduck') {
        // MotherDuck is DuckDB-native, no account/warehouse/role layer.
        // Just a database name plus an optional inline token (otherwise
        // the runtime falls back to the MOTHERDUCK_TOKEN env var).
        return base(comp, [
            {
                label: 'MotherDuck',
                fields: [
                    { key: 'database', label: 'Database', kind: 'text', required: true, placeholder: 'my_db' },
                    {
                        key: 'token',
                        label: 'MotherDuck token',
                        kind: 'text',
                        description: 'Optional. If empty, MOTHERDUCK_TOKEN from the environment is used.',
                    },
                    { key: 'schemaName', label: 'Schema', kind: 'text', defaultValue: 'main' },
                    { key: 'tableName', label: 'Table', kind: 'text', required: true, placeholder: 'orders' },
                ],
            },
        ]);
    }
    return base(comp, [
        {
            label: 'Account',
            fields: [
                { key: 'account', label: 'Account identifier', kind: 'text', required: true, placeholder: 'xy12345.us-east-1' },
                { key: 'warehouse', label: 'Warehouse', kind: 'text' },
                { key: 'role', label: 'Role', kind: 'text' },
                { key: 'database', label: 'Database', kind: 'text', required: true },
                { key: 'schema', label: 'Schema', kind: 'text' },
                ...credentialFields(),
            ],
        },
        { label: 'Query', fields: dbReadFields() },
    ]);
}

function synthWarehouseSink(comp: ComponentDef): ComponentManifest {
    return base(
        comp,
        [
            {
                label: 'Account',
                fields: [
                    { key: 'account', label: 'Account identifier', kind: 'text', required: true },
                    { key: 'warehouse', label: 'Warehouse', kind: 'text' },
                    { key: 'role', label: 'Role', kind: 'text' },
                    { key: 'database', label: 'Database', kind: 'text', required: true },
                    { key: 'schema', label: 'Schema', kind: 'text' },
                    ...credentialFields(),
                ],
            },
            { label: 'Destination', fields: dbWriteFields() },
        ],
        'upstream',
    );
}

function synthStorageSource(comp: ComponentDef): ComponentManifest {
    return base(comp, [
        {
            label: 'Object',
            fields: [
                { key: 'bucket', label: 'Bucket', kind: 'text', required: true, placeholder: 'my-bucket' },
                { key: 'key', label: 'Key / prefix', kind: 'text', required: true, placeholder: 'data/orders.parquet' },
                { key: 'region', label: 'Region', kind: 'text', placeholder: 'us-east-1' },
                {
                    key: 'glob',
                    label: 'Read as prefix glob',
                    kind: 'bool',
                    defaultValue: false,
                },
            ],
        },
        {
            label: 'Credentials',
            fields: [
                { key: 'accessKey', label: 'Access key', kind: 'text' },
                { key: 'secretKey', label: 'Secret key', kind: 'text', placeholder: '••••••••' },
                { key: 'sessionToken', label: 'Session token', kind: 'text' },
                {
                    key: 'connectionRef',
                    label: 'Or use saved connection',
                    kind: 'connection-ref',
                    accepts: ['s3', 'gcs', 'azure-blob'],
                },
            ],
        },
        {
            label: 'S3-compatible (MinIO / R2 / B2)',
            fields: [
                {
                    key: 'endpoint',
                    label: 'Endpoint',
                    kind: 'text',
                    description: 'host:port for MinIO; the provider host for R2 / B2. Leave empty for plain AWS S3.',
                    placeholder: 'localhost:9000',
                },
                {
                    key: 'urlStyle',
                    label: 'URL style',
                    kind: 'select',
                    defaultValue: '',
                    options: [
                        { label: 'Default', value: '' },
                        { label: 'Path (MinIO / B2)', value: 'path' },
                        { label: 'Virtual host (R2 / AWS)', value: 'vhost' },
                    ],
                },
                {
                    key: 'useSsl',
                    label: 'Use TLS',
                    kind: 'select',
                    defaultValue: '',
                    options: [
                        { label: 'Default (true)', value: '' },
                        { label: 'true', value: 'true' },
                        { label: 'false (local MinIO)', value: 'false' },
                    ],
                },
            ],
        },
        {
            label: 'Format',
            fields: [
                {
                    key: 'format',
                    label: 'File format',
                    kind: 'select',
                    defaultValue: 'parquet',
                    options: [
                        { label: 'Parquet', value: 'parquet' },
                        { label: 'CSV', value: 'csv' },
                        { label: 'JSON', value: 'json' },
                        { label: 'JSONL', value: 'jsonl' },
                        { label: 'Avro', value: 'avro' },
                        { label: 'ORC', value: 'orc' },
                    ],
                },
            ],
        },
    ]);
}

function synthStorageSink(comp: ComponentDef): ComponentManifest {
    return base(
        comp,
        [
            {
                label: 'Object',
                fields: [
                    { key: 'bucket', label: 'Bucket', kind: 'text', required: true },
                    { key: 'key', label: 'Key / prefix', kind: 'text', required: true },
                    { key: 'region', label: 'Region', kind: 'text' },
                ],
            },
            {
                label: 'Credentials',
                fields: [
                    { key: 'accessKey', label: 'Access key', kind: 'text' },
                    { key: 'secretKey', label: 'Secret key', kind: 'text', placeholder: '••••••••' },
                    {
                        key: 'connectionRef',
                        label: 'Or use saved connection',
                        kind: 'connection-ref',
                        accepts: ['s3', 'gcs', 'azure-blob'],
                    },
                ],
            },
            {
                label: 'Format',
                fields: [
                    {
                        key: 'format',
                        label: 'File format',
                        kind: 'select',
                        defaultValue: 'parquet',
                        options: [
                            { label: 'Parquet', value: 'parquet' },
                            { label: 'CSV', value: 'csv' },
                            { label: 'JSON', value: 'json' },
                            { label: 'JSONL', value: 'jsonl' },
                            { label: 'Avro', value: 'avro' },
                            { label: 'ORC', value: 'orc' },
                        ],
                    },
                    writeModeField(),
                    compressionField(),
                    {
                        key: 'partitionBy',
                        label: 'Partition by columns',
                        kind: 'columns',
                        description: 'Hive-style partitioned output.',
                    },
                ],
            },
        ],
        'upstream',
    );
}

function synthStreamingSource(comp: ComponentDef): ComponentManifest {
    return base(comp, [
        {
            label: 'Broker',
            fields: [
                {
                    key: 'brokers',
                    label: 'Bootstrap servers',
                    kind: 'text',
                    required: true,
                    placeholder: 'broker1:9092,broker2:9092',
                },
                { key: 'topic', label: 'Topic', kind: 'text', required: true },
                { key: 'groupId', label: 'Consumer group', kind: 'text', placeholder: 'duckle-group' },
                {
                    key: 'offset',
                    label: 'Initial offset',
                    kind: 'select',
                    defaultValue: 'latest',
                    options: [
                        { label: 'Latest', value: 'latest' },
                        { label: 'Earliest', value: 'earliest' },
                    ],
                },
            ],
        },
        {
            label: 'Security',
            fields: [
                {
                    key: 'security',
                    label: 'Security protocol',
                    kind: 'select',
                    defaultValue: 'plaintext',
                    options: [
                        { label: 'PLAINTEXT', value: 'plaintext' },
                        { label: 'SSL', value: 'ssl' },
                        { label: 'SASL_SSL', value: 'sasl_ssl' },
                        { label: 'SASL_PLAINTEXT', value: 'sasl_plaintext' },
                    ],
                },
                { key: 'saslMechanism', label: 'SASL mechanism', kind: 'text' },
                { key: 'saslUsername', label: 'SASL username', kind: 'text' },
                { key: 'saslPassword', label: 'SASL password', kind: 'text', placeholder: '••••••••' },
            ],
        },
        {
            label: 'Format',
            fields: [
                {
                    key: 'format',
                    label: 'Message format',
                    kind: 'select',
                    defaultValue: 'json',
                    options: [
                        { label: 'JSON', value: 'json' },
                        { label: 'Avro', value: 'avro' },
                        { label: 'Protobuf', value: 'protobuf' },
                        { label: 'Plain text', value: 'text' },
                    ],
                },
                { key: 'schemaRegistryUrl', label: 'Schema Registry URL', kind: 'text' },
            ],
        },
    ]);
}

function synthStreamingSink(comp: ComponentDef): ComponentManifest {
    return base(
        comp,
        [
            {
                label: 'Broker',
                fields: [
                    {
                        key: 'brokers',
                        label: 'Bootstrap servers',
                        kind: 'text',
                        required: true,
                    },
                    { key: 'topic', label: 'Topic', kind: 'text', required: true },
                    { key: 'acks', label: 'Acks', kind: 'select', defaultValue: 'all',
                      options: [{label:'all',value:'all'},{label:'1',value:'1'},{label:'0',value:'0'}] },
                ],
            },
            {
                label: 'Format',
                fields: [
                    {
                        key: 'format',
                        label: 'Message format',
                        kind: 'select',
                        defaultValue: 'json',
                        options: [
                            { label: 'JSON', value: 'json' },
                            { label: 'Avro', value: 'avro' },
                            { label: 'Protobuf', value: 'protobuf' },
                        ],
                    },
                    { key: 'keyColumn', label: 'Message key column', kind: 'column' },
                ],
            },
        ],
        'upstream',
    );
}

function synthApiSource(comp: ComponentDef): ComponentManifest {
    return base(comp, [
        {
            label: 'Request',
            fields: [
                { key: 'url', label: 'URL', kind: 'text', required: true, placeholder: 'https://api.example.com/v1/resource' },
                {
                    key: 'method',
                    label: 'Method',
                    kind: 'select',
                    defaultValue: 'GET',
                    options: [
                        { label: 'GET', value: 'GET' },
                        { label: 'POST', value: 'POST' },
                        { label: 'PUT', value: 'PUT' },
                        { label: 'DELETE', value: 'DELETE' },
                    ],
                },
                { key: 'headers', label: 'Headers', kind: 'key-value' },
                { key: 'body', label: 'Request body', kind: 'textarea', rows: 4 },
            ],
        },
        {
            label: 'Auth',
            fields: [
                {
                    key: 'authType',
                    label: 'Auth type',
                    kind: 'select',
                    defaultValue: 'none',
                    options: [
                        { label: 'None', value: 'none' },
                        { label: 'Basic', value: 'basic' },
                        { label: 'Bearer token', value: 'bearer' },
                        { label: 'API key (header)', value: 'apikey' },
                        { label: 'OAuth2', value: 'oauth2' },
                    ],
                },
                { key: 'authToken', label: 'Token / API key', kind: 'text', placeholder: '••••••••' },
            ],
        },
        {
            label: 'Response',
            fields: [
                {
                    key: 'jsonPath',
                    label: 'Records JSONPath',
                    kind: 'text',
                    placeholder: '$.data[*]',
                    description: 'Where in the response body to pull records from.',
                },
                {
                    key: 'pagination',
                    label: 'Pagination',
                    kind: 'select',
                    defaultValue: 'none',
                    options: [
                        { label: 'None', value: 'none' },
                        { label: 'Page number', value: 'page' },
                        { label: 'Offset / limit', value: 'offset' },
                        { label: 'Cursor', value: 'cursor' },
                        { label: 'Link header', value: 'link' },
                    ],
                },
            ],
        },
    ]);
}

function synthApiSink(comp: ComponentDef): ComponentManifest {
    return base(
        comp,
        [
            {
                label: 'Request',
                fields: [
                    { key: 'url', label: 'URL', kind: 'text', required: true },
                    {
                        key: 'method',
                        label: 'Method',
                        kind: 'select',
                        defaultValue: 'POST',
                        options: [
                            { label: 'POST', value: 'POST' },
                            { label: 'PUT', value: 'PUT' },
                            { label: 'PATCH', value: 'PATCH' },
                        ],
                    },
                    { key: 'headers', label: 'Headers', kind: 'key-value' },
                    {
                        key: 'batchMode',
                        label: 'Batch mode',
                        kind: 'select',
                        defaultValue: 'one',
                        options: [
                            { label: 'One request per row', value: 'one' },
                            { label: 'Batch into array', value: 'array' },
                        ],
                    },
                ],
            },
            {
                label: 'Auth',
                fields: [
                    {
                        key: 'authType',
                        label: 'Auth type',
                        kind: 'select',
                        defaultValue: 'none',
                        options: [
                            { label: 'None', value: 'none' },
                            { label: 'Bearer', value: 'bearer' },
                            { label: 'API key', value: 'apikey' },
                        ],
                    },
                    { key: 'authToken', label: 'Token', kind: 'text', placeholder: '••••••••' },
                ],
            },
        ],
        'upstream',
    );
}

function synthNoSqlSource(comp: ComponentDef): ComponentManifest {
    return base(comp, [
        {
            label: 'Connection',
            fields: [
                { key: 'connectionString', label: 'Connection string', kind: 'text', required: true,
                  placeholder: 'mongodb://localhost:27017' },
                { key: 'database', label: 'Database', kind: 'text', required: true },
                { key: 'collection', label: 'Collection / Index', kind: 'text', required: true },
            ],
        },
        {
            label: 'Query',
            fields: [
                {
                    key: 'queryMode',
                    label: 'Query mode',
                    kind: 'select',
                    defaultValue: 'all',
                    options: [
                        { label: 'All documents', value: 'all' },
                        { label: 'Filter query', value: 'filter' },
                        { label: 'Aggregation pipeline', value: 'aggregation' },
                    ],
                },
                { key: 'filter', label: 'Filter (JSON)', kind: 'textarea', rows: 4, placeholder: '{"status": "active"}' },
                { key: 'projection', label: 'Projection (JSON)', kind: 'textarea', rows: 3 },
                { key: 'limit', label: 'Limit', kind: 'integer' },
            ],
        },
    ]);
}

function synthNoSqlSink(comp: ComponentDef): ComponentManifest {
    return base(
        comp,
        [
            {
                label: 'Connection',
                fields: [
                    { key: 'connectionString', label: 'Connection string', kind: 'text', required: true },
                    { key: 'database', label: 'Database', kind: 'text', required: true },
                    { key: 'collection', label: 'Collection / Index', kind: 'text', required: true },
                ],
            },
            {
                label: 'Write',
                fields: [
                    {
                        key: 'mode',
                        label: 'Write mode',
                        kind: 'select',
                        defaultValue: 'insert',
                        options: [
                            { label: 'Insert', value: 'insert' },
                            { label: 'Upsert', value: 'upsert' },
                            { label: 'Replace', value: 'replace' },
                            { label: 'Delete', value: 'delete' },
                        ],
                    },
                    { key: 'idColumn', label: 'ID column', kind: 'column' },
                    { key: 'batchSize', label: 'Batch size', kind: 'integer', defaultValue: 1000 },
                ],
            },
        ],
        'upstream',
    );
}

function synthMiscSource(comp: ComponentDef): ComponentManifest {
    const id = comp.id;
    if (id === 'src.ftp') {
        return base(comp, [
            {
                label: 'Connection',
                fields: [
                    { key: 'protocol', label: 'Protocol', kind: 'select', defaultValue: 'sftp',
                      options: [{label:'SFTP',value:'sftp'},{label:'FTP',value:'ftp'},{label:'FTPS',value:'ftps'}] },
                    { key: 'host', label: 'Host', kind: 'text', required: true },
                    { key: 'port', label: 'Port', kind: 'integer', defaultValue: 22 },
                    { key: 'username', label: 'Username', kind: 'text' },
                    { key: 'password', label: 'Password', kind: 'text', placeholder: '••••••••' },
                    { key: 'privateKeyPath', label: 'Private key file', kind: 'file-path' },
                    { key: 'remotePath', label: 'Remote path', kind: 'text', required: true,
                      placeholder: '/incoming/orders.csv' },
                ],
            },
        ]);
    }
    if (id === 'src.http') {
        return synthApiSource(comp);
    }
    if (id === 'src.email') {
        return base(comp, [
            {
                label: 'IMAP',
                fields: [
                    { key: 'host', label: 'IMAP host', kind: 'text', required: true },
                    { key: 'port', label: 'Port', kind: 'integer', defaultValue: 993 },
                    { key: 'username', label: 'Username', kind: 'text', required: true },
                    { key: 'password', label: 'Password', kind: 'text', placeholder: '••••••••' },
                    { key: 'folder', label: 'Folder', kind: 'text', defaultValue: 'INBOX' },
                    {
                        key: 'filter',
                        label: 'Search criteria',
                        kind: 'text',
                        placeholder: 'UNSEEN',
                    },
                ],
            },
        ]);
    }
    if (id === 'src.git') {
        return base(comp, [
            {
                label: 'Repository',
                fields: [
                    { key: 'url', label: 'Repository URL', kind: 'text', required: true },
                    { key: 'branch', label: 'Branch', kind: 'text', defaultValue: 'main' },
                    { key: 'path', label: 'File path in repo', kind: 'text' },
                    { key: 'authToken', label: 'Access token', kind: 'text', placeholder: '••••••••' },
                ],
            },
        ]);
    }
    return synthGeneric(comp);
}

// Transforms ------------------------------------------------------------

function synthFieldsTransform(comp: ComponentDef): ComponentManifest {
    const id = comp.id;
    const schemaSource: SchemaSource = id === 'xf.map' || id === 'xf.project' ? 'declared' : 'upstream';

    if (id === 'xf.cast') {
        return base(comp, [
            {
                label: 'Type conversion',
                fields: [
                    { key: 'column', label: 'Column', kind: 'column', required: true },
                    {
                        key: 'targetType',
                        label: 'Target type',
                        kind: 'select',
                        defaultValue: 'string',
                        options: [
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
                        ],
                    },
                    {
                        key: 'onError',
                        label: 'On conversion error',
                        kind: 'select',
                        defaultValue: 'null',
                        options: [
                            { label: 'Set to NULL', value: 'null' },
                            { label: 'Reject row', value: 'reject' },
                            { label: 'Fail pipeline', value: 'fail' },
                        ],
                    },
                ],
            },
        ], schemaSource);
    }
    if (id === 'xf.rename') {
        return base(comp, [
            { label: 'Rename', fields: [{ key: 'mapping', label: 'Old → New', kind: 'key-value', description: 'old column name → new column name' }] },
        ], schemaSource);
    }
    if (id === 'xf.addcol' || id === 'xf.coalesce') {
        return base(comp, [
            {
                label: 'New column',
                fields: [
                    { key: 'name', label: 'Column name', kind: 'text', required: true },
                    {
                        key: 'type',
                        label: 'Type',
                        kind: 'select',
                        defaultValue: 'string',
                        options: [
                            { label: 'string', value: 'string' },
                            { label: 'int64', value: 'int64' },
                            { label: 'float64', value: 'float64' },
                            { label: 'bool', value: 'bool' },
                            { label: 'timestamp', value: 'timestamp' },
                        ],
                    },
                    { key: 'expression', label: 'Expression', kind: 'expression', rows: 3,
                      placeholder: id === 'xf.coalesce' ? "COALESCE(col_a, col_b, 'default')" : 'amount * 1.08' },
                ],
            },
        ], 'declared');
    }
    if (id === 'xf.dropcol') {
        return base(comp, [
            { label: 'Drop', fields: [{ key: 'columns', label: 'Columns to drop', kind: 'columns', required: true }] },
        ], schemaSource);
    }
    if (id === 'xf.reorder') {
        return base(comp, [
            { label: 'Reorder', fields: [{ key: 'columns', label: 'Columns in new order', kind: 'columns', required: true }] },
        ], schemaSource);
    }
    return synthGeneric(comp, schemaSource);
}

function synthRowTransform(comp: ComponentDef): ComponentManifest {
    const id = comp.id;
    if (id === 'xf.sort') {
        return base(comp, [
            {
                label: 'Sort',
                fields: [
                    { key: 'column', label: 'Column', kind: 'column', required: true },
                    {
                        key: 'direction',
                        label: 'Direction',
                        kind: 'select',
                        defaultValue: 'asc',
                        options: [{label:'Ascending',value:'asc'},{label:'Descending',value:'desc'}],
                    },
                    { key: 'nullsLast', label: 'NULLs last', kind: 'bool', defaultValue: true },
                ],
            },
        ], 'upstream');
    }
    if (id === 'xf.distinct') {
        return base(comp, [
            { label: 'Distinct', fields: [{ key: 'columns', label: 'Columns', kind: 'columns', description: 'Leave empty to dedupe on the whole row.' }] },
        ], 'upstream');
    }
    if (id === 'xf.topn' || id === 'xf.sample' || id === 'xf.skip') {
        return base(comp, [
            {
                label: id === 'xf.sample' ? 'Sample' : 'Limit',
                fields: [
                    { key: 'count', label: id === 'xf.sample' ? 'Sample size' : 'Row count', kind: 'integer', defaultValue: 100 },
                    ...(id === 'xf.sample' ? [{
                        key: 'method', label: 'Method', kind: 'select' as const, defaultValue: 'random',
                        options: [{label:'Random',value:'random'},{label:'Reservoir',value:'reservoir'},{label:'Bernoulli',value:'bernoulli'}],
                    }] : []),
                ],
            },
        ], 'upstream');
    }
    return synthGeneric(comp, 'upstream');
}

function synthAggregateTransform(comp: ComponentDef): ComponentManifest {
    if (comp.id === 'xf.aggwin') {
        // Window aggregate: an aggregate over a window that keeps every row.
        return base(comp, [
            {
                label: 'Window aggregate',
                fields: [
                    {
                        key: 'function',
                        label: 'Function',
                        kind: 'select',
                        defaultValue: 'sum',
                        options: ['sum', 'avg', 'count', 'min', 'max'].map(f => ({
                            label: f.toUpperCase(),
                            value: f,
                        })),
                    },
                    { key: 'column', label: 'Column', kind: 'column', required: true },
                    { key: 'partitionBy', label: 'Partition by', kind: 'columns' },
                    { key: 'orderBy', label: 'Order by', kind: 'columns' },
                    { key: 'outputName', label: 'Output column', kind: 'text', defaultValue: 'window_value' },
                ],
            },
        ], 'declared');
    }
    return base(comp, [
        { label: 'Grouping', fields: [{ key: 'groupKeys', label: 'Group by', kind: 'columns', required: true }] },
        { label: 'Aggregations', fields: [{ key: 'aggregations', label: 'Aggregations', kind: 'aggregations', required: true }] },
        { label: 'Filter', fields: [{ key: 'havingClause', label: 'HAVING clause', kind: 'expression', rows: 2, placeholder: 'sum_amount > 1000' }] },
    ], 'declared');
}

function synthJoinTransform(comp: ComponentDef): ComponentManifest {
    const joinType = comp.id.split('.').pop() ?? 'inner';
    return base(comp, [
        {
            label: 'Join keys',
            fields: [
                { key: 'leftKey', label: 'Left key', kind: 'text', required: true, placeholder: 'orders.customer_id' },
                { key: 'rightKey', label: 'Right key', kind: 'text', required: true, placeholder: 'customers.id' },
                { key: 'multipleKeys', label: 'Multi-column key (left,right pairs)', kind: 'key-value' },
            ],
        },
        {
            label: 'Join type',
            fields: [
                {
                    key: 'joinType',
                    label: 'Type',
                    kind: 'select',
                    defaultValue: joinType,
                    options: [
                        { label: 'INNER', value: 'inner' },
                        { label: 'LEFT', value: 'left' },
                        { label: 'RIGHT', value: 'right' },
                        { label: 'FULL OUTER', value: 'full' },
                        { label: 'CROSS', value: 'cross' },
                        { label: 'SEMI', value: 'semi' },
                        { label: 'ANTI', value: 'anti' },
                    ],
                },
                {
                    key: 'sendUnmatchedToReject',
                    label: 'Send unmatched to reject port',
                    kind: 'bool',
                    defaultValue: false,
                },
            ],
        },
    ], 'declared');
}

function synthSetTransform(comp: ComponentDef): ComponentManifest {
    return base(comp, [
        {
            label: 'Set operation',
            fields: [
                {
                    key: 'matchBy',
                    label: 'Column match',
                    kind: 'select',
                    defaultValue: 'name',
                    options: [
                        { label: 'By column name', value: 'name' },
                        { label: 'By position', value: 'position' },
                    ],
                },
            ],
        },
    ], 'upstream');
}

function synthWindowTransform(comp: ComponentDef): ComponentManifest {
    const fn = comp.id.split('.').pop() ?? 'rownum';
    return base(comp, [
        {
            label: 'Window function',
            fields: [
                {
                    key: 'function',
                    label: 'Function',
                    kind: 'select',
                    defaultValue: fn,
                    options: [
                        { label: 'ROW_NUMBER', value: 'rownum' },
                        { label: 'RANK', value: 'rank' },
                        { label: 'DENSE_RANK', value: 'denserank' },
                        { label: 'LEAD', value: 'lead' },
                        { label: 'LAG', value: 'lag' },
                        { label: 'FIRST_VALUE', value: 'first' },
                        { label: 'LAST_VALUE', value: 'last' },
                        { label: 'NTILE', value: 'ntile' },
                    ],
                },
                { key: 'targetColumn', label: 'Target column (lead/lag/first/last)', kind: 'column' },
                { key: 'offset', label: 'Offset (lead/lag)', kind: 'integer', defaultValue: 1 },
            ],
        },
        {
            label: 'Window',
            fields: [
                { key: 'partitionBy', label: 'Partition by', kind: 'columns' },
                { key: 'orderBy', label: 'Order by', kind: 'columns' },
                {
                    key: 'frame',
                    label: 'Frame',
                    kind: 'text',
                    placeholder: 'ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW',
                },
            ],
        },
        { label: 'Output', fields: [{ key: 'outputName', label: 'Output column name', kind: 'text', required: true, placeholder: 'row_num' }] },
    ], 'declared');
}

function synthStringTransform(comp: ComponentDef): ComponentManifest {
    return base(comp, [
        {
            label: 'String operation',
            fields: [
                { key: 'column', label: 'Column', kind: 'column', required: true },
                { key: 'pattern', label: 'Pattern / args', kind: 'text' },
                { key: 'replacement', label: 'Replacement', kind: 'text' },
                { key: 'outputColumn', label: 'Output column', kind: 'text', placeholder: 'leave blank to overwrite' },
            ],
        },
    ], 'upstream');
}

const TIME_UNITS = ['year', 'quarter', 'month', 'week', 'day', 'hour', 'minute', 'second'];
const unitField = (label: string): Field => ({
    key: 'unit',
    label,
    kind: 'select',
    defaultValue: 'day',
    options: TIME_UNITS.map(u => ({ label: u, value: u })),
});
const outColField = (placeholder = 'leave blank to replace the column'): Field => ({
    key: 'outputColumn',
    label: 'Output column',
    kind: 'text',
    placeholder,
});

function synthDateTimeTransform(comp: ComponentDef): ComponentManifest {
    const id = comp.id;
    const col: Field = { key: 'column', label: 'Date column', kind: 'column', required: true };
    if (id === 'xf.dt.parse') {
        return base(comp, [{ label: 'Parse date', fields: [
            col,
            { key: 'format', label: 'Format pattern', kind: 'text', defaultValue: '%Y-%m-%d', placeholder: '%Y-%m-%d %H:%M:%S' },
            outColField(),
        ] }], 'upstream');
    }
    if (id === 'xf.dt.format') {
        return base(comp, [{ label: 'Format date', fields: [
            col,
            { key: 'format', label: 'Format pattern', kind: 'text', defaultValue: '%Y-%m-%d', placeholder: '%Y-%m-%d' },
            outColField(),
        ] }], 'upstream');
    }
    if (id === 'xf.dt.extract') {
        return base(comp, [{ label: 'Extract part', fields: [col, unitField('Part'), outColField()] }], 'upstream');
    }
    if (id === 'xf.dt.trunc') {
        return base(comp, [{ label: 'Truncate', fields: [col, unitField('Truncate to'), outColField()] }], 'upstream');
    }
    if (id === 'xf.dt.tz') {
        return base(comp, [{ label: 'Timezone convert', fields: [
            col,
            { key: 'timezone', label: 'Timezone', kind: 'text', required: true, placeholder: 'America/New_York' },
            outColField(),
        ] }], 'upstream');
    }
    if (id === 'xf.dt.add') {
        return base(comp, [{ label: 'Date add', fields: [
            col,
            { key: 'amount', label: 'Amount (negative subtracts)', kind: 'integer', defaultValue: 1 },
            unitField('Unit'),
            outColField(),
        ] }], 'upstream');
    }
    if (id === 'xf.dt.diff') {
        return base(comp, [{ label: 'Date diff', fields: [
            { key: 'startColumn', label: 'Start column', kind: 'column', required: true },
            { key: 'endColumn', label: 'End column', kind: 'column', required: true },
            unitField('Unit'),
            { key: 'outputColumn', label: 'Output column', kind: 'text', defaultValue: 'date_diff' },
        ] }], 'upstream');
    }
    return synthGeneric(comp, 'upstream');
}

function synthNumericTransform(comp: ComponentDef): ComponentManifest {
    return base(comp, [
        {
            label: 'Numeric operation',
            fields: [
                { key: 'column', label: 'Column', kind: 'column', required: true },
                { key: 'argument', label: 'Argument', kind: 'text' },
                { key: 'outputColumn', label: 'Output column', kind: 'text' },
            ],
        },
    ], 'upstream');
}

function synthPivotTransform(comp: ComponentDef): ComponentManifest {
    if (comp.id === 'xf.unpivot') {
        // Unpivot: wide to long. Chosen columns become name/value rows.
        return base(comp, [
            {
                label: 'Unpivot',
                fields: [
                    { key: 'columns', label: 'Columns to unpivot', kind: 'columns', required: true },
                    { key: 'nameColumn', label: 'Name column', kind: 'text', defaultValue: 'name' },
                    { key: 'valueColumn', label: 'Value column', kind: 'text', defaultValue: 'value' },
                ],
            },
        ], 'declared');
    }
    if (comp.id === 'xf.denorm') {
        // Denormalize: collapse child rows into one row per group, joining
        // the chosen columns into delimited cells.
        return base(comp, [
            {
                label: 'Denormalize',
                fields: [
                    { key: 'groupBy', label: 'Group by', kind: 'columns', required: true },
                    { key: 'aggregateColumns', label: 'Aggregate columns (joined into one cell)', kind: 'columns', required: true },
                    { key: 'separator', label: 'Separator', kind: 'text', defaultValue: ', ' },
                ],
            },
        ], 'declared');
    }
    if (comp.id === 'xf.norm') {
        // Normalize: explode a delimited / array column into one row per
        // element, keeping the rest of the row intact.
        return base(comp, [
            {
                label: 'Normalize',
                fields: [
                    { key: 'column', label: 'Column to split', kind: 'column', required: true },
                    { key: 'separator', label: 'Separator', kind: 'text', defaultValue: ',', description: 'Leave empty if the column is already an array.' },
                ],
            },
        ], 'upstream');
    }
    if (comp.id === 'xf.transpose') {
        // Transpose: swap rows and columns. Requires the input's columns
        // to share a compatible type.
        return base(comp, [{ label: 'Transpose', fields: [] }], 'declared');
    }
    return base(comp, [
        {
            label: 'Pivot',
            fields: [
                { key: 'pivotColumn', label: 'Pivot column (becomes columns)', kind: 'column', required: true },
                { key: 'valueColumn', label: 'Value column (filled in cells)', kind: 'column', required: true },
                { key: 'groupBy', label: 'Group by (rows)', kind: 'columns' },
                {
                    key: 'aggregation',
                    label: 'Aggregation',
                    kind: 'select',
                    defaultValue: 'sum',
                    options: ['sum','count','avg','min','max','first','last'].map(o=>({label:o.toUpperCase(),value:o})),
                },
            ],
        },
    ], 'declared');
}

function synthJsonTransform(comp: ComponentDef): ComponentManifest {
    const id = comp.id;
    const col: Field = { key: 'column', label: 'JSON column', kind: 'column', required: true };
    if (id === 'xf.json.path') {
        return base(comp, [{ label: 'JSONPath extract', fields: [
            col,
            { key: 'path', label: 'JSONPath', kind: 'text', required: true, placeholder: '$.user.email' },
            outColField(),
        ] }], 'upstream');
    }
    if (id === 'xf.json.flatten') {
        return base(comp, [{ label: 'Flatten', fields: [
            { key: 'column', label: 'Struct column to flatten', kind: 'column', required: true,
              description: "Expands the struct's fields into top-level columns." },
        ] }], 'declared');
    }
    if (id === 'xf.json.merge') {
        return base(comp, [{ label: 'Merge objects', fields: [
            { key: 'column', label: 'First JSON column', kind: 'column', required: true },
            { key: 'secondColumn', label: 'Second JSON column', kind: 'column', required: true },
            { key: 'outputColumn', label: 'Output column', kind: 'text', defaultValue: 'merged' },
        ] }], 'upstream');
    }
    // parse / stringify
    return base(comp, [{ label: 'JSON operation', fields: [col, outColField()] }], 'upstream');
}

function synthArrayTransform(comp: ComponentDef): ComponentManifest {
    const id = comp.id;
    const col: Field = { key: 'column', label: 'Array column', kind: 'column', required: true };
    if (id === 'xf.arr.element') {
        return base(comp, [{ label: 'Element at', fields: [
            col,
            { key: 'index', label: 'Index (1-based)', kind: 'integer', defaultValue: 1 },
            outColField(),
        ] }], 'upstream');
    }
    if (id === 'xf.arr.contains') {
        return base(comp, [{ label: 'Contains', fields: [
            col,
            { key: 'value', label: 'Value to find', kind: 'text', required: true },
            { key: 'outputColumn', label: 'Output column', kind: 'text', placeholder: 'e.g. has_value' },
        ] }], 'upstream');
    }
    if (id === 'xf.arr.collect') {
        return base(comp, [{ label: 'Collect list', fields: [
            { key: 'valueColumn', label: 'Value column', kind: 'column', required: true },
            { key: 'groupBy', label: 'Group by', kind: 'columns', description: 'Leave empty to collect all rows into one list.' },
            { key: 'outputColumn', label: 'Output column', kind: 'text', defaultValue: 'items' },
        ] }], 'declared');
    }
    if (id === 'xf.arr.explode') {
        return base(comp, [{ label: 'Explode / Unnest', fields: [
            { key: 'column', label: 'Array column', kind: 'column', required: true,
              description: 'One output row per element, other columns repeated.' },
        ] }], 'declared');
    }
    // distinct
    return base(comp, [{ label: 'Array distinct', fields: [col, outColField()] }], 'upstream');
}

function synthCdcTransform(comp: ComponentDef): ComponentManifest {
    const id = comp.id;
    return base(comp, [
        {
            label: 'Keys',
            fields: [
                { key: 'naturalKey', label: 'Natural key columns', kind: 'columns', required: true },
                { key: 'compareColumns', label: 'Columns to compare', kind: 'columns' },
            ],
        },
        {
            label: id.endsWith('.scd2') ? 'SCD Type 2 columns' : 'Behavior',
            fields: id.endsWith('.scd2')
                ? [
                      { key: 'validFromColumn', label: 'Valid-from column', kind: 'text', defaultValue: 'valid_from' },
                      { key: 'validToColumn', label: 'Valid-to column', kind: 'text', defaultValue: 'valid_to' },
                      { key: 'isCurrentColumn', label: 'Is-current flag column', kind: 'text', defaultValue: 'is_current' },
                  ]
                : [{ key: 'rejectUnchanged', label: 'Drop unchanged rows', kind: 'bool', defaultValue: true }],
        },
    ], 'declared');
}

// Control / Quality / Custom -------------------------------------------

function synthRoutingControl(comp: ComponentDef): ComponentManifest {
    const id = comp.id;
    if (id === 'ctl.switch') {
        return base(comp, [
            {
                label: 'Branches',
                fields: [
                    { key: 'branches', label: 'Branch conditions', kind: 'key-value',
                      description: 'branch_name → boolean expression. Rows go down the first matching branch.' },
                    { key: 'defaultBranch', label: 'Default branch name', kind: 'text', defaultValue: 'else' },
                ],
            },
        ], 'upstream');
    }
    if (id === 'ctl.replicate' || id === 'ctl.merge') {
        return base(comp, [{ label: id === 'ctl.merge' ? 'Merge streams' : 'Replicate', fields: [] }], 'upstream');
    }
    if (id === 'ctl.iterate' || id === 'ctl.foreach') {
        return base(comp, [
            {
                label: 'Loop',
                fields: [
                    { key: 'variable', label: 'Loop variable', kind: 'text', placeholder: 'i' },
                    { key: 'from', label: 'From', kind: 'integer', defaultValue: 0 },
                    { key: 'to', label: 'To', kind: 'integer', defaultValue: 10 },
                    { key: 'collection', label: 'Or iterate over column', kind: 'column' },
                ],
            },
        ], 'upstream');
    }
    return synthGeneric(comp, 'upstream');
}

function synthTimingControl(comp: ComponentDef): ComponentManifest {
    const id = comp.id;
    if (id === 'ctl.wait') {
        return base(comp, [
            {
                label: 'Delay',
                fields: [
                    { key: 'duration', label: 'Duration', kind: 'integer', defaultValue: 1 },
                    {
                        key: 'unit',
                        label: 'Unit',
                        kind: 'select',
                        defaultValue: 'seconds',
                        options: ['milliseconds','seconds','minutes','hours'].map(u=>({label:u,value:u})),
                    },
                ],
            },
        ], 'upstream');
    }
    if (id === 'ctl.schedule') {
        return base(comp, [
            {
                label: 'Schedule',
                fields: [
                    { key: 'cron', label: 'Cron expression', kind: 'text', placeholder: '0 0 * * *' },
                    { key: 'timezone', label: 'Timezone', kind: 'text', defaultValue: 'UTC' },
                ],
            },
        ], 'upstream');
    }
    if (id === 'ctl.throttle') {
        return base(comp, [
            {
                label: 'Throttle',
                fields: [
                    { key: 'rate', label: 'Rows per second', kind: 'integer', defaultValue: 100 },
                ],
            },
        ], 'upstream');
    }
    return synthGeneric(comp, 'upstream');
}

function synthPipelineControl(comp: ComponentDef): ComponentManifest {
    if (comp.id === 'ctl.runpipeline' || comp.id === 'ctl.trigger') {
        return base(comp, [
            {
                label: 'Pipeline',
                fields: [
                    { key: 'pipelineRef', label: 'Pipeline', kind: 'text', required: true, placeholder: 'pipelines/customers_sync' },
                    { key: 'waitForCompletion', label: 'Wait for completion', kind: 'bool', defaultValue: true },
                    { key: 'parameters', label: 'Parameters', kind: 'key-value' },
                ],
            },
        ], 'upstream');
    }
    if (comp.id === 'ctl.checkpoint') {
        return base(comp, [
            {
                label: 'Checkpoint',
                fields: [
                    { key: 'name', label: 'Checkpoint name', kind: 'text', required: true },
                    { key: 'storage', label: 'Storage path', kind: 'save-path' },
                ],
            },
        ], 'upstream');
    }
    return synthGeneric(comp, 'upstream');
}

function synthErrorControl(comp: ComponentDef): ComponentManifest {
    const id = comp.id;
    if (id === 'ctl.retry') {
        return base(comp, [
            {
                label: 'Retry',
                fields: [
                    { key: 'maxAttempts', label: 'Max attempts', kind: 'integer', defaultValue: 3 },
                    { key: 'backoff', label: 'Backoff (ms)', kind: 'integer', defaultValue: 1000 },
                    {
                        key: 'strategy',
                        label: 'Strategy',
                        kind: 'select',
                        defaultValue: 'exponential',
                        options: [{label:'Linear',value:'linear'},{label:'Exponential',value:'exponential'},{label:'Constant',value:'constant'}],
                    },
                ],
            },
        ], 'upstream');
    }
    if (id === 'ctl.deadletter') {
        return base(comp, [
            {
                label: 'Dead letter',
                fields: [
                    { key: 'destination', label: 'Destination', kind: 'save-path', required: true },
                    {
                        key: 'format',
                        label: 'Format',
                        kind: 'select',
                        defaultValue: 'json',
                        options: [{label:'JSON',value:'json'},{label:'CSV',value:'csv'},{label:'Parquet',value:'parquet'}],
                    },
                ],
            },
        ], 'upstream');
    }
    return synthGeneric(comp, 'upstream');
}

function synthQualityValidation(comp: ComponentDef): ComponentManifest {
    const id = comp.id;
    const onFail: Field = {
        key: 'onFail',
        label: 'On failure',
        kind: 'select',
        defaultValue: 'reject',
        options: [
            { label: 'Send to reject port', value: 'reject' },
            { label: 'Log warning, keep row', value: 'warn' },
            { label: 'Fail pipeline', value: 'fail' },
        ],
    };
    if (id === 'qa.schemavalidate') {
        return base(comp, [
            {
                label: 'Schema',
                fields: [
                    {
                        key: 'expectedColumns',
                        label: 'Expected columns',
                        kind: 'columns',
                        description: 'Rows missing or with extra columns fail.',
                    },
                    onFail,
                ],
            },
        ], 'upstream');
    }
    if (id === 'qa.regex') {
        return base(comp, [
            {
                label: 'Regex match',
                fields: [
                    { key: 'column', label: 'Column', kind: 'column', required: true },
                    { key: 'pattern', label: 'Regex pattern', kind: 'text', required: true, placeholder: '^[A-Z]{2}\\d{6}$' },
                    onFail,
                ],
            },
        ], 'upstream');
    }
    if (id === 'qa.range') {
        return base(comp, [
            {
                label: 'Range',
                fields: [
                    { key: 'column', label: 'Column', kind: 'column', required: true },
                    { key: 'min', label: 'Min', kind: 'number' },
                    { key: 'max', label: 'Max', kind: 'number' },
                    { key: 'inclusive', label: 'Inclusive bounds', kind: 'bool', defaultValue: true },
                    onFail,
                ],
            },
        ], 'upstream');
    }
    if (id === 'qa.notnull') {
        return base(comp, [
            {
                label: 'Not-null',
                fields: [
                    { key: 'columns', label: 'Required columns', kind: 'columns', required: true },
                    onFail,
                ],
            },
        ], 'upstream');
    }
    if (id === 'qa.unique') {
        return base(comp, [
            {
                label: 'Uniqueness',
                fields: [
                    { key: 'columns', label: 'Uniqueness key', kind: 'columns', required: true },
                    onFail,
                ],
            },
        ], 'upstream');
    }
    return synthGeneric(comp, 'upstream');
}

function synthQualityProfile(comp: ComponentDef): ComponentManifest {
    if (comp.id === 'qa.describe') {
        // Outputs the input's column names and types; no configuration.
        return base(comp, [{ label: 'Describe', fields: [] }], 'declared');
    }
    if (comp.id === 'qa.histogram') {
        // Outputs value/frequency rows for one column.
        return base(comp, [{
            label: 'Histogram',
            fields: [{ key: 'column', label: 'Column', kind: 'column', required: true }],
        }], 'declared');
    }
    return base(comp, [
        {
            label: 'Profile',
            fields: [
                { key: 'columns', label: 'Columns to profile', kind: 'columns', description: 'Leave empty for all.' },
            ],
        },
    ], 'declared');
}

function synthQualityCleanse(comp: ComponentDef): ComponentManifest {
    const id = comp.id;
    if (id === 'qa.standardize') {
        return base(comp, [
            {
                label: 'Standardize',
                fields: [
                    { key: 'columns', label: 'Columns', kind: 'columns', required: true },
                    {
                        key: 'case',
                        label: 'Case',
                        kind: 'select',
                        defaultValue: 'none',
                        options: [
                            { label: 'Keep as-is', value: 'none' },
                            { label: 'UPPERCASE', value: 'upper' },
                            { label: 'lowercase', value: 'lower' },
                            { label: 'Title Case', value: 'title' },
                        ],
                    },
                    { key: 'trim', label: 'Trim whitespace', kind: 'bool', defaultValue: true },
                    { key: 'collapseWhitespace', label: 'Collapse inner whitespace', kind: 'bool', defaultValue: true },
                ],
            },
        ], 'upstream');
    }
    if (id === 'qa.dedupe' || id === 'qa.match') {
        const isMatch = id === 'qa.match';
        return base(comp, [
            {
                label: isMatch ? 'Record match' : 'Fuzzy deduplicate',
                fields: [
                    { key: 'columns', label: 'Compare columns', kind: 'columns', required: true },
                    { key: 'threshold', label: 'Similarity threshold', kind: 'number', defaultValue: 0.85, description: '0.0 to 1.0; higher is stricter.' },
                    {
                        key: 'algorithm',
                        label: 'Algorithm',
                        kind: 'select',
                        defaultValue: 'jaro-winkler',
                        options: [
                            { label: 'Jaro-Winkler', value: 'jaro-winkler' },
                            { label: 'Levenshtein', value: 'levenshtein' },
                        ],
                    },
                ],
            },
        ], isMatch ? 'declared' : 'upstream');
    }
    return base(comp, [
        {
            label: 'Cleanse',
            fields: [
                { key: 'columns', label: 'Columns', kind: 'columns' },
                { key: 'rules', label: 'Rules', kind: 'key-value', description: 'rule_name → pattern_or_value' },
            ],
        },
    ], 'upstream');
}

function synthCustomCode(comp: ComponentDef): ComponentManifest {
    const id = comp.id;
    if (id === 'code.sql' || id === 'code.sqltemplate') {
        return base(comp, [
            {
                label: 'SQL',
                fields: [
                    {
                        key: 'routineRef',
                        label: 'Use saved SQL routine (optional)',
                        kind: 'routine-ref',
                        accepts: ['sql'],
                        description: 'Pick a saved SQL routine, or write inline below.',
                    },
                    {
                        key: 'sql',
                        label: 'SQL',
                        kind: 'expression',
                        rows: 10,
                        placeholder: 'SELECT *, upper(status) AS status FROM input',
                        description: 'The upstream rows are available as `input`.',
                    },
                ],
            },
        ], 'declared');
    }
    const langDefault =
        id === 'code.python' ? 'python' :
        id === 'code.rust' ? 'rust' :
        id === 'code.javascript' ? 'javascript' :
        id === 'code.shell' ? 'bash' :
        id === 'code.wasm' ? 'wasm' : 'plain';
    return base(comp, [
        {
            label: 'Code',
            fields: [
                {
                    key: 'routineRef',
                    label: 'Use saved routine (optional)',
                    kind: 'routine-ref',
                    accepts: [langDefault],
                    description: 'Pick a saved routine, or write inline below.',
                },
                {
                    key: 'language',
                    label: 'Language',
                    kind: 'select',
                    defaultValue: langDefault,
                    options: [
                        { label: 'Python', value: 'python' },
                        { label: 'Rust', value: 'rust' },
                        { label: 'JavaScript', value: 'javascript' },
                        { label: 'Bash', value: 'bash' },
                        { label: 'WASM', value: 'wasm' },
                    ],
                },
                { key: 'code', label: 'Source', kind: 'textarea', rows: 12, monospace: true, required: true,
                  placeholder: id === 'code.python' ? 'def process(row):\n    return row' : '// custom code' },
                ...(id === 'code.wasm' ? [{ key: 'wasmPath', label: 'WASM file', kind: 'file-path' as const,
                    filters: [{ name: 'WebAssembly', extensions: ['wasm'] }] }] : []),
            ],
        },
    ], 'declared');
}

// AI / Vector ----------------------------------------------------------

const aiProviderField = (): Field => ({
    key: 'provider',
    label: 'Provider',
    kind: 'select',
    defaultValue: 'openai',
    options: [
        { label: 'OpenAI', value: 'openai' },
        { label: 'Anthropic', value: 'anthropic' },
        { label: 'Cohere', value: 'cohere' },
        { label: 'Hugging Face', value: 'huggingface' },
        { label: 'Local (Ollama)', value: 'ollama' },
    ],
});

const distanceMetricField = (): Field => ({
    key: 'metric',
    label: 'Distance metric',
    kind: 'select',
    defaultValue: 'cosine',
    options: [
        { label: 'Cosine', value: 'cosine' },
        { label: 'Euclidean (L2)', value: 'l2' },
        { label: 'Dot product', value: 'dot' },
    ],
});

function synthVectorSink(comp: ComponentDef): ComponentManifest {
    return base(
        comp,
        [
            {
                label: 'Vector store',
                fields: [
                    { key: 'endpoint', label: 'Endpoint / host', kind: 'text', placeholder: 'http://localhost:6333' },
                    { key: 'apiKey', label: 'API key', kind: 'text', placeholder: '••••••••' },
                    { key: 'collection', label: 'Collection / index', kind: 'text', required: true, placeholder: 'documents' },
                    { key: 'connectionRef', label: 'Or use saved connection', kind: 'connection-ref' },
                ],
            },
            {
                label: 'Vectors',
                fields: [
                    { key: 'embeddingColumn', label: 'Embedding column', kind: 'column', required: true, description: 'Column holding the vector (array of floats).' },
                    { key: 'idColumn', label: 'ID column', kind: 'column' },
                    { key: 'metadataColumns', label: 'Metadata columns', kind: 'columns', description: 'Stored alongside each vector.' },
                    { key: 'dimension', label: 'Dimensions', kind: 'integer', defaultValue: 1536 },
                    distanceMetricField(),
                    {
                        key: 'mode',
                        label: 'Write mode',
                        kind: 'select',
                        defaultValue: 'upsert',
                        options: [
                            { label: 'Upsert', value: 'upsert' },
                            { label: 'Insert', value: 'insert' },
                        ],
                    },
                    { key: 'batchSize', label: 'Batch size', kind: 'integer', defaultValue: 100 },
                    { key: 'createIfMissing', label: 'Create collection if missing', kind: 'bool', defaultValue: true },
                ],
            },
        ],
        'upstream',
    );
}

function synthVectorSource(comp: ComponentDef): ComponentManifest {
    return base(comp, [
        {
            label: 'Vector store',
            fields: [
                { key: 'endpoint', label: 'Endpoint / host', kind: 'text' },
                { key: 'apiKey', label: 'API key', kind: 'text', placeholder: '••••••••' },
                { key: 'collection', label: 'Collection / index', kind: 'text', required: true },
                { key: 'connectionRef', label: 'Or use saved connection', kind: 'connection-ref' },
            ],
        },
        {
            label: 'Query',
            fields: [
                {
                    key: 'queryMode',
                    label: 'Mode',
                    kind: 'select',
                    defaultValue: 'fetch',
                    options: [
                        { label: 'Fetch all', value: 'fetch' },
                        { label: 'Similarity search', value: 'search' },
                    ],
                },
                { key: 'queryText', label: 'Query text', kind: 'textarea', rows: 2, description: 'For similarity search.' },
                { key: 'topK', label: 'Top K', kind: 'integer', defaultValue: 10 },
                { key: 'filter', label: 'Metadata filter (JSON)', kind: 'textarea', rows: 3, placeholder: '{"source": "docs"}' },
            ],
        },
    ]);
}

function synthAiTransform(comp: ComponentDef): ComponentManifest {
    const id = comp.id;
    if (id === 'xf.ai.embed') {
        return base(comp, [
            {
                label: 'Embeddings',
                fields: [
                    { key: 'textColumn', label: 'Text column', kind: 'column', required: true },
                    aiProviderField(),
                    { key: 'model', label: 'Model', kind: 'text', defaultValue: 'text-embedding-3-small' },
                    { key: 'apiKey', label: 'API key', kind: 'text', placeholder: '••••••••' },
                    { key: 'outputColumn', label: 'Output column', kind: 'text', defaultValue: 'embedding' },
                    { key: 'dimension', label: 'Dimensions', kind: 'integer', defaultValue: 1536 },
                    { key: 'batchSize', label: 'Batch size', kind: 'integer', defaultValue: 64 },
                ],
            },
        ], 'declared');
    }
    if (id === 'xf.ai.llm') {
        return base(comp, [
            {
                label: 'Model',
                fields: [
                    aiProviderField(),
                    { key: 'model', label: 'Model', kind: 'text', defaultValue: 'gpt-4o-mini' },
                    { key: 'apiKey', label: 'API key', kind: 'text', placeholder: '••••••••' },
                ],
            },
            {
                label: 'Prompt',
                fields: [
                    {
                        key: 'prompt',
                        label: 'Prompt template',
                        kind: 'textarea',
                        rows: 6,
                        monospace: true,
                        required: true,
                        placeholder: 'Clean and normalize this address:\n{{address}}',
                        description: 'Reference columns with {{column_name}}.',
                    },
                    { key: 'outputColumn', label: 'Output column', kind: 'text', required: true, defaultValue: 'ai_result' },
                    { key: 'temperature', label: 'Temperature', kind: 'number', defaultValue: 0 },
                    { key: 'maxTokens', label: 'Max tokens', kind: 'integer', defaultValue: 256 },
                ],
            },
        ], 'declared');
    }
    if (id === 'xf.ai.chunk') {
        return base(comp, [
            {
                label: 'Chunking',
                fields: [
                    { key: 'textColumn', label: 'Text column', kind: 'column', required: true },
                    {
                        key: 'strategy',
                        label: 'Strategy',
                        kind: 'select',
                        defaultValue: 'recursive',
                        options: [
                            { label: 'Fixed size', value: 'fixed' },
                            { label: 'Sentence', value: 'sentence' },
                            { label: 'Recursive', value: 'recursive' },
                            { label: 'Semantic', value: 'semantic' },
                        ],
                    },
                    { key: 'chunkSize', label: 'Chunk size (tokens)', kind: 'integer', defaultValue: 512 },
                    { key: 'overlap', label: 'Overlap (tokens)', kind: 'integer', defaultValue: 64 },
                    { key: 'outputColumn', label: 'Output column', kind: 'text', defaultValue: 'chunk' },
                ],
            },
        ], 'declared');
    }
    if (id === 'xf.ai.pii') {
        return base(comp, [
            {
                label: 'PII redaction',
                fields: [
                    { key: 'columns', label: 'Columns to scan', kind: 'columns', required: true },
                    { key: 'entities', label: 'Entity types', kind: 'text', placeholder: 'email, phone, ssn, name, credit_card', description: 'Comma-separated PII types to detect.' },
                    {
                        key: 'action',
                        label: 'Action',
                        kind: 'select',
                        defaultValue: 'mask',
                        options: [
                            { label: 'Mask (****)', value: 'mask' },
                            { label: 'Hash', value: 'hash' },
                            { label: 'Redact (remove)', value: 'redact' },
                            { label: 'Tokenize', value: 'tokenize' },
                        ],
                    },
                ],
            },
        ], 'upstream');
    }
    if (id === 'xf.ai.classify') {
        return base(comp, [
            {
                label: 'Classify',
                fields: [
                    { key: 'textColumn', label: 'Text column', kind: 'column', required: true },
                    { key: 'labels', label: 'Labels', kind: 'text', required: true, placeholder: 'positive, neutral, negative', description: 'Comma-separated candidate labels.' },
                    aiProviderField(),
                    { key: 'model', label: 'Model', kind: 'text', defaultValue: 'gpt-4o-mini' },
                    { key: 'apiKey', label: 'API key', kind: 'text', placeholder: '••••••••' },
                    { key: 'outputColumn', label: 'Output column', kind: 'text', defaultValue: 'label' },
                ],
            },
        ], 'declared');
    }
    if (id === 'xf.ai.dedupe') {
        return base(comp, [
            {
                label: 'Semantic dedupe',
                fields: [
                    { key: 'embeddingColumn', label: 'Embedding column', kind: 'column', description: 'Vector column to compare.' },
                    { key: 'textColumn', label: 'Or text column', kind: 'column', description: 'Embedded on the fly if no vector column.' },
                    { key: 'threshold', label: 'Similarity threshold', kind: 'number', defaultValue: 0.92, description: '0.0-1.0; higher keeps only very-close rows as duplicates.' },
                    distanceMetricField(),
                    {
                        key: 'keep',
                        label: 'Keep',
                        kind: 'select',
                        defaultValue: 'first',
                        options: [
                            { label: 'First', value: 'first' },
                            { label: 'Last', value: 'last' },
                        ],
                    },
                ],
            },
        ], 'upstream');
    }
    return synthGeneric(comp, 'upstream');
}

function synthDebugTransform(comp: ComponentDef): ComponentManifest {
    if (comp.id === 'xf.log') {
        return base(comp, [
            {
                label: 'Log Rows',
                fields: [
                    {
                        key: 'label',
                        label: 'Log label',
                        kind: 'text',
                        placeholder: 'after-filter',
                        description: 'Shown in the Output / Console next to the logged rows.',
                    },
                    {
                        key: 'limit',
                        label: 'Max rows to print',
                        kind: 'integer',
                        defaultValue: 100,
                    },
                    {
                        key: 'columns',
                        label: 'Columns (blank = all)',
                        kind: 'columns',
                    },
                ],
            },
        ], 'upstream');
    }
    return synthGeneric(comp, 'upstream');
}

function synthGeneric(comp: ComponentDef, schemaSource: SchemaSource = 'upstream'): ComponentManifest {
    return base(comp, [
        {
            label: 'Settings',
            fields: [
                { key: 'notes', label: 'Notes', kind: 'textarea', rows: 3, description: 'Component-specific configuration not yet modeled - describe the intent here.' },
            ],
        },
    ], schemaSource);
}

// Main entry ------------------------------------------------------------

export function synthesizeManifest(componentId: string): ComponentManifest | undefined {
    const entry = findPaletteEntry(componentId);
    if (!entry) return undefined;
    const { groupId, comp } = entry;

    // Sources
    if (groupId === 'src.files') return synthFileSource(comp);
    if (groupId === 'src.lakehouse') return synthLakehouseSource(comp);
    if (groupId === 'src.databases') return synthDbSource(comp);
    if (groupId === 'src.warehouses') return synthWarehouseSource(comp);
    if (groupId === 'src.storage') return synthStorageSource(comp);
    if (groupId === 'src.streaming') return synthStreamingSource(comp);
    if (groupId === 'src.apis') return synthApiSource(comp);
    if (groupId === 'src.nosql') return synthNoSqlSource(comp);
    if (groupId === 'src.misc') return synthMiscSource(comp);
    if (groupId === 'src.vector') return synthVectorSource(comp);

    // Sinks
    if (groupId === 'snk.files') return synthFileSink(comp);
    if (groupId === 'snk.databases') return synthDbSink(comp);
    if (groupId === 'snk.warehouses') return synthWarehouseSink(comp);
    if (groupId === 'snk.storage') return synthStorageSink(comp);
    if (groupId === 'snk.streaming') return synthStreamingSink(comp);
    if (groupId === 'snk.apis') return synthApiSink(comp);
    if (groupId === 'snk.nosql') return synthNoSqlSink(comp);
    if (groupId === 'snk.vector') return synthVectorSink(comp);

    // Transforms
    if (groupId === 'xf.fields') return synthFieldsTransform(comp);
    if (groupId === 'xf.rows') return synthRowTransform(comp);
    if (groupId === 'xf.aggregate') return synthAggregateTransform(comp);
    if (groupId === 'xf.join') return synthJoinTransform(comp);
    if (groupId === 'xf.set') return synthSetTransform(comp);
    if (groupId === 'xf.window') return synthWindowTransform(comp);
    if (groupId === 'xf.strings') return synthStringTransform(comp);
    if (groupId === 'xf.datetime') return synthDateTimeTransform(comp);
    if (groupId === 'xf.numeric') return synthNumericTransform(comp);
    if (groupId === 'xf.pivot') return synthPivotTransform(comp);
    if (groupId === 'xf.json') return synthJsonTransform(comp);
    if (groupId === 'xf.array') return synthArrayTransform(comp);
    if (groupId === 'xf.cdc') return synthCdcTransform(comp);
    if (groupId === 'xf.ai') return synthAiTransform(comp);
    if (groupId === 'xf.debug') return synthDebugTransform(comp);

    // Control
    if (groupId === 'ctl.routing') return synthRoutingControl(comp);
    if (groupId === 'ctl.timing') return synthTimingControl(comp);
    if (groupId === 'ctl.pipeline') return synthPipelineControl(comp);
    if (groupId === 'ctl.errors') return synthErrorControl(comp);

    // Quality
    if (groupId === 'qa.validation') return synthQualityValidation(comp);
    if (groupId === 'qa.profile') return synthQualityProfile(comp);
    if (groupId === 'qa.cleanse') return synthQualityCleanse(comp);

    // Custom code
    if (groupId === 'code.sql') return synthCustomCode(comp);
    if (groupId === 'code.scripts') return synthCustomCode(comp);

    // SaaS - treat as API sources for now
    if (groupId.startsWith('saas.')) return synthApiSource(comp);

    return synthGeneric(comp);
}
