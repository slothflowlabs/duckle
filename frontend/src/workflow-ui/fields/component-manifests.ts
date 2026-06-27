import type { ComponentManifest, AutodetectFn } from './types';
import type { Column } from '../../pipeline-types';
import { synthesizeManifest, portsForComponent } from './manifest-synth';
import { PALETTE } from '../palette-data';
import { tauriAutodetect } from '../../tauri-bridge';

const CSV_SAMPLE_SCHEMA: Column[] = [
    { name: 'order_id', type: 'int64', nullable: false, primaryKey: true },
    { name: 'customer_id', type: 'int64', nullable: false },
    { name: 'status', type: 'string', nullable: false },
    { name: 'amount', type: 'decimal', nullable: true },
    { name: 'currency', type: 'string', nullable: false },
    { name: 'created_at', type: 'timestamp', nullable: false },
];

const CSV_SAMPLE_ROWS = [
    { order_id: 1001, customer_id: 42, status: 'paid', amount: 129.95, currency: 'USD', created_at: '2026-05-18T14:23:11Z' },
    { order_id: 1002, customer_id: 17, status: 'pending', amount: 49.0, currency: 'USD', created_at: '2026-05-18T14:24:02Z' },
    { order_id: 1003, customer_id: 42, status: 'paid', amount: 12.5, currency: 'USD', created_at: '2026-05-18T14:25:47Z' },
    { order_id: 1004, customer_id: 99, status: 'refunded', amount: 200.0, currency: 'EUR', created_at: '2026-05-18T14:30:18Z' },
];

const PARQUET_SAMPLE_SCHEMA: Column[] = [
    { name: 'event_id', type: 'string', nullable: false, primaryKey: true },
    { name: 'user_id', type: 'int64', nullable: false },
    { name: 'event_type', type: 'string', nullable: false },
    { name: 'event_time', type: 'timestamp', nullable: false },
    { name: 'properties', type: 'json', nullable: true },
];

const PARQUET_SAMPLE_ROWS = [
    { event_id: 'e_a8f3', user_id: 42, event_type: 'page_view', event_time: '2026-05-18T14:23:11Z', properties: '{"path":"/home"}' },
    { event_id: 'e_b2d7', user_id: 17, event_type: 'click', event_time: '2026-05-18T14:23:18Z', properties: '{"target":"cta"}' },
];

const SQLITE_SAMPLE_SCHEMA: Column[] = [
    { name: 'id', type: 'int64', nullable: false, primaryKey: true },
    { name: 'name', type: 'string', nullable: false },
    { name: 'email', type: 'string', nullable: true },
    { name: 'created_at', type: 'timestamp', nullable: false },
];

const JSON_SAMPLE_SCHEMA: Column[] = [
    { name: 'id', type: 'string', nullable: false },
    { name: 'payload', type: 'json', nullable: true },
    { name: 'received_at', type: 'timestamp', nullable: false },
];


function realOrMockAutodetect(
    format: string,
    mockColumns: Column[],
    mockRows: Record<string, unknown>[] = [],
): AutodetectFn {
    return async (props: Record<string, unknown>) => {
        // Different connectors carry the "where to look" key under
        // different names. Treat any non-empty location as a signal to
        // hit the real Rust path.
        const hasLocation =
            stringy(props.path) ||
            stringy(props.database) ||
            stringy(props.url) ||
            stringy(props.host);
        if (hasLocation) {
            const real = await tauriAutodetect(format, props);
            if (real) return { columns: real.columns, sampleRows: real.sampleRows };
        }
        await new Promise(r => setTimeout(r, 250));
        return { columns: mockColumns, sampleRows: mockRows };
    };
}

function stringy(v: unknown): boolean {
    return typeof v === 'string' && v.trim().length > 0;
}

export const MANIFESTS: Record<string, ComponentManifest> = {
    'src.csv': {
        id: 'src.csv',
        kind: 'source',
        label: 'CSV',
        description: 'Read delimited text files.',
        schemaSource: 'autodetect',
        autodetect: realOrMockAutodetect('csv', CSV_SAMPLE_SCHEMA, CSV_SAMPLE_ROWS),
        sections: [
            {
                label: 'Source file',
                fields: [
                    {
                        key: 'path',
                        label: 'Path',
                        kind: 'file-path',
                        required: true,
                        placeholder: 'e.g. C:\\data\\orders.csv',
                        filters: [
                            { name: 'CSV / TSV', extensions: ['csv', 'tsv', 'txt'] },
                            { name: 'All files', extensions: ['*'] },
                        ],
                    },
                    {
                        key: 'hasHeader',
                        label: 'First row is header',
                        kind: 'bool',
                        defaultValue: true,
                        placeholder: 'Use the first row as column names',
                    },
                    {
                        key: 'delimiter',
                        label: 'Delimiter',
                        kind: 'select',
                        defaultValue: ',',
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
                    {
                        key: 'encoding',
                        label: 'Encoding',
                        kind: 'select',
                        defaultValue: 'utf-8',
                        options: [
                            { label: 'UTF-8', value: 'utf-8' },
                            { label: 'UTF-16', value: 'utf-16' },
                            { label: 'Latin-1 (ISO-8859-1)', value: 'latin-1' },
                            { label: 'Windows-1252', value: 'windows-1252' },
                        ],
                    },
                    {
                        key: 'skipLines',
                        label: 'Skip lines (top)',
                        kind: 'integer',
                        defaultValue: 0,
                    },
                    {
                        key: 'nullValue',
                        label: 'Null sentinel',
                        kind: 'text',
                        placeholder: 'e.g. NULL, NA, \\N',
                        description: 'Strings that should be interpreted as NULL.',
                    },
                ],
            },
        ],
    },

    'src.parquet': {
        id: 'src.parquet',
        kind: 'source',
        label: 'Parquet',
        description: 'Read columnar Parquet files.',
        schemaSource: 'autodetect',
        autodetect: realOrMockAutodetect('parquet', PARQUET_SAMPLE_SCHEMA, PARQUET_SAMPLE_ROWS),
        sections: [
            {
                label: 'Source file',
                fields: [
                    {
                        key: 'path',
                        label: 'Path',
                        kind: 'file-path',
                        required: true,
                        filters: [
                            { name: 'Parquet', extensions: ['parquet', 'pq'] },
                            { name: 'All files', extensions: ['*'] },
                        ],
                    },
                    {
                        key: 'columns',
                        label: 'Projection (columns to read)',
                        kind: 'text',
                        placeholder: 'leave blank for all columns',
                        description: 'Comma-separated; pushed down to the Parquet reader.',
                    },
                    {
                        key: 'rowGroupRange',
                        label: 'Row group range',
                        kind: 'text',
                        placeholder: 'e.g. 0..10',
                    },
                ],
            },
        ],
    },

    'src.sqlite': {
        id: 'src.sqlite',
        kind: 'source',
        label: 'SQLite',
        description: 'Read from a SQLite database file.',
        schemaSource: 'autodetect',
        autodetect: realOrMockAutodetect('sqlite', SQLITE_SAMPLE_SCHEMA),
        sections: [
            {
                label: 'Connection',
                fields: [
                    {
                        key: 'database',
                        label: 'Database file',
                        kind: 'file-path',
                        required: true,
                        filters: [
                            { name: 'SQLite', extensions: ['db', 'sqlite', 'sqlite3'] },
                            { name: 'All files', extensions: ['*'] },
                        ],
                    },
                ],
            },
            {
                label: 'Query',
                fields: [
                    {
                        key: 'mode',
                        label: 'Read mode',
                        kind: 'select',
                        defaultValue: 'table',
                        options: [
                            { label: 'Whole table', value: 'table' },
                            { label: 'Custom SQL', value: 'sql' },
                        ],
                    },
                    {
                        key: 'tableName',
                        label: 'Table name',
                        kind: 'text',
                        placeholder: 'users',
                    },
                    {
                        key: 'sql',
                        label: 'SQL query',
                        kind: 'expression',
                        rows: 5,
                        placeholder: 'SELECT * FROM users WHERE created_at > ?',
                    },
                ],
            },
        ],
    },

    'src.adbc': {
        id: 'src.adbc',
        kind: 'source',
        label: 'ADBC (Arrow)',
        description:
            'Read any database that ships an ADBC driver. Load a prebuilt driver shared library at runtime, connect via a URI, and run SQL; rows stream back as Arrow.',
        schemaSource: 'declared',
        sections: [
            {
                label: 'Driver',
                fields: [
                    {
                        key: 'driver',
                        label: 'Driver library',
                        kind: 'file-path',
                        required: true,
                        placeholder: 'e.g. C:\\drivers\\adbc_driver_sqlite.dll',
                        description: 'Path to the prebuilt ADBC driver shared library (.dll / .so / .dylib). Any dependent libraries must sit next to it.',
                        filters: [
                            { name: 'Shared library', extensions: ['dll', 'so', 'dylib'] },
                            { name: 'All files', extensions: ['*'] },
                        ],
                    },
                    {
                        key: 'entrypoint',
                        label: 'Init entrypoint (optional)',
                        kind: 'text',
                        placeholder: 'AdbcDriverInit',
                        description: 'Custom driver init symbol. Leave blank for the standard AdbcDriverInit.',
                    },
                ],
            },
            {
                label: 'Connection',
                fields: [
                    {
                        key: 'uri',
                        label: 'URI',
                        kind: 'text',
                        placeholder: 'a database file path or a server URI',
                        description: 'Passed to the driver as the ADBC uri option. Driver-specific: a file path for SQLite, a DSN / URL for server drivers.',
                    },
                    {
                        key: 'options',
                        label: 'Driver options',
                        kind: 'key-value',
                        description: 'Extra ADBC database options (username, password, and any driver-specific keys).',
                    },
                ],
            },
            {
                label: 'Query',
                fields: [
                    {
                        key: 'query',
                        label: 'SQL query',
                        kind: 'expression',
                        rows: 5,
                        required: true,
                        placeholder: 'SELECT * FROM my_table',
                    },
                ],
            },
        ],
        ports: {
            inputs: [],
            outputs: [
                { id: 'main', label: 'main', type: 'main' },
                { id: 'reject', label: 'reject', type: 'reject', optional: true },
            ],
        },
    },

    'src.gizmosql': {
        id: 'src.gizmosql',
        kind: 'source',
        label: 'GizmoSQL',
        description:
            'Query a GizmoSQL (Arrow Flight SQL) server. Pure-Rust Flight SQL client: rows stream back as Arrow and materialize fast - no ADBC driver or JDBC needed.',
        schemaSource: 'declared',
        sections: [
            {
                label: 'Connection',
                fields: [
                    { key: 'host', label: 'Host', kind: 'text', required: true, placeholder: 'localhost' },
                    { key: 'port', label: 'Port', kind: 'integer', defaultValue: 31337 },
                    { key: 'username', label: 'Username', kind: 'text', placeholder: 'gizmosql_username or ${ENV:GIZMOSQL_USER}' },
                    { key: 'password', label: 'Password', kind: 'text', placeholder: '${ENV:GIZMOSQL_PASSWORD}' },
                    { key: 'tls', label: 'Use TLS', kind: 'bool', defaultValue: false },
                    { key: 'tlsSkipVerify', label: 'Skip TLS verification (self-signed)', kind: 'bool', defaultValue: false },
                ],
            },
            {
                label: 'Query',
                fields: [
                    { key: 'query', label: 'SQL query', kind: 'expression', rows: 5, required: true, placeholder: 'SELECT * FROM my_table' },
                ],
            },
        ],
        ports: {
            inputs: [],
            outputs: [
                { id: 'main', label: 'main', type: 'main' },
                { id: 'reject', label: 'reject', type: 'reject', optional: true },
            ],
        },
    },

    'snk.gizmosql': {
        id: 'snk.gizmosql',
        kind: 'sink',
        label: 'GizmoSQL',
        description:
            'Write rows to a table on a GizmoSQL (Arrow Flight SQL) server via CREATE + batched INSERT over the pure-Rust Flight SQL client.',
        schemaSource: 'declared',
        sections: [
            {
                label: 'Connection',
                fields: [
                    { key: 'host', label: 'Host', kind: 'text', required: true, placeholder: 'localhost' },
                    { key: 'port', label: 'Port', kind: 'integer', defaultValue: 31337 },
                    { key: 'username', label: 'Username', kind: 'text', placeholder: 'gizmosql_username or ${ENV:GIZMOSQL_USER}' },
                    { key: 'password', label: 'Password', kind: 'text', placeholder: '${ENV:GIZMOSQL_PASSWORD}' },
                    { key: 'tls', label: 'Use TLS', kind: 'bool', defaultValue: false },
                    { key: 'tlsSkipVerify', label: 'Skip TLS verification (self-signed)', kind: 'bool', defaultValue: false },
                ],
            },
            {
                label: 'Target',
                fields: [
                    { key: 'table', label: 'Table', kind: 'text', required: true, placeholder: 'my_table' },
                    {
                        key: 'mode',
                        label: 'Write mode',
                        kind: 'select',
                        defaultValue: 'append',
                        options: [
                            { label: 'Append (create if missing)', value: 'append' },
                            { label: 'Overwrite (replace table)', value: 'overwrite' },
                        ],
                    },
                ],
            },
        ],
        ports: {
            inputs: [
                { id: 'main', label: 'main', type: 'main' },
            ],
            outputs: [],
        },
    },

    'src.s3': {
        id: 'src.s3',
        kind: 'source',
        label: 'Amazon S3',
        description: 'Read CSV / Parquet / JSON from an s3:// URI via DuckDB httpfs.',
        schemaSource: 'autodetect',
        autodetect: realOrMockAutodetect('s3', CSV_SAMPLE_SCHEMA),
        sections: [
            {
                label: 'Source',
                fields: [
                    {
                        key: 'path',
                        label: 'S3 URI',
                        kind: 'text',
                        required: true,
                        placeholder: 's3://bucket/path/to/file.parquet',
                        description: 'Full S3 URI. File format is inferred from the extension.',
                    },
                    {
                        key: 'connectionRef',
                        label: 'Or use saved connection',
                        kind: 'connection-ref',
                        accepts: ['s3'],
                    },
                    {
                        key: 'format',
                        label: 'Format override',
                        kind: 'select',
                        options: [
                            { label: 'Auto-detect from extension', value: '' },
                            { label: 'CSV', value: 'csv' },
                            { label: 'Parquet', value: 'parquet' },
                            { label: 'JSON', value: 'json' },
                        ],
                    },
                ],
            },
        ],
        ports: { inputs: [], outputs: [{ id: 'main', label: 'out', type: 'main' }] },
    },

    'src.duckdb': {
        id: 'src.duckdb',
        kind: 'source',
        label: 'DuckDB',
        description: 'Read from a DuckDB database file.',
        schemaSource: 'autodetect',
        autodetect: realOrMockAutodetect('duckdb', CSV_SAMPLE_SCHEMA),
        sections: [
            {
                label: 'Connection',
                fields: [
                    {
                        key: 'database',
                        label: 'Database file',
                        kind: 'file-path',
                        required: true,
                        filters: [
                            { name: 'DuckDB', extensions: ['duckdb', 'db'] },
                            { name: 'All files', extensions: ['*'] },
                        ],
                    },
                ],
            },
            {
                label: 'Source table',
                fields: [
                    {
                        // Not required: the custom-SQL field below is a
                        // documented alternative ("used only when no table is
                        // set above"), and build_duckdb_source falls back to
                        // `sql` when no table is given. Forcing a table here
                        // wrongly rejected valid custom-SQL reads.
                        key: 'tableName',
                        label: 'Table',
                        kind: 'text',
                        placeholder: 'orders',
                    },
                    {
                        key: 'schema',
                        label: 'Schema',
                        kind: 'text',
                        placeholder: 'main',
                    },
                    {
                        key: 'sql',
                        label: 'Advanced: custom SQL',
                        kind: 'expression',
                        rows: 4,
                        placeholder: 'SELECT * FROM duckle_src.orders WHERE status = ...',
                        description:
                            'Optional - used only when no table is set above. Reference tables as duckle_src.<table>.',
                    },
                ],
            },
        ],
    },

    'src.json': {
        id: 'src.json',
        kind: 'source',
        label: 'JSON',
        description: 'Read JSON or NDJSON files.',
        schemaSource: 'autodetect',
        autodetect: realOrMockAutodetect('json', JSON_SAMPLE_SCHEMA),
        sections: [
            {
                label: 'Source file',
                fields: [
                    {
                        key: 'path',
                        label: 'Path',
                        kind: 'file-path',
                        required: true,
                        filters: [
                            { name: 'JSON', extensions: ['json', 'jsonl', 'ndjson'] },
                            { name: 'All files', extensions: ['*'] },
                        ],
                    },
                    {
                        key: 'format',
                        label: 'Format',
                        kind: 'select',
                        defaultValue: 'auto',
                        options: [
                            { label: 'Auto-detect', value: 'auto' },
                            { label: 'JSON array', value: 'array' },
                            { label: 'JSON Lines', value: 'jsonl' },
                            { label: 'Single object', value: 'object' },
                        ],
                    },
                    {
                        key: 'flatten',
                        label: 'Flatten nested objects',
                        kind: 'bool',
                        defaultValue: false,
                    },
                    {
                        key: 'recordsPath',
                        label: 'Records path',
                        kind: 'text',
                        placeholder: 'data   or   response.records',
                        description:
                            "Dotted key path to the array of records inside the JSON, for API-style responses where the rows live under a key (e.g. {\"data\":[...]} -> 'data', or {\"response\":{\"records\":[...]}} -> 'response.records'). Each record is unnested and nested fields are flattened into columns. Leave blank for a plain top-level array or JSON Lines.",
                    },
                    {
                        key: 'ignoreErrors',
                        label: 'Skip malformed records',
                        kind: 'bool',
                        defaultValue: false,
                        description:
                            'Skip records DuckDB cannot parse instead of failing the whole load (#101). Best for large JSON Lines files where one bad line should not abort the run; the error message names the offending line and byte.',
                    },
                ],
            },
        ],
    },

    'xf.filter': {
        id: 'xf.filter',
        kind: 'transform',
        label: 'Filter Rows',
        description: 'Keep rows that match a predicate.',
        schemaSource: 'upstream',
        sections: [
            {
                label: 'Filter',
                fields: [
                    {
                        key: 'predicate',
                        label: 'Predicate',
                        kind: 'filter-predicate',
                        required: true,
                        description:
                            'Visual builder with column / operator / value, or raw SQL. Rows where the predicate is true are kept.',
                    },
                    {
                        key: 'rejectOnError',
                        label: 'Send errors to reject port',
                        kind: 'bool',
                        defaultValue: false,
                    },
                ],
            },
        ],
    },

    'xf.project': {
        id: 'xf.project',
        kind: 'transform',
        label: 'Project / Select',
        description: 'Pick which columns to keep, in which order.',
        schemaSource: 'upstream',
        sections: [
            {
                label: 'Columns',
                fields: [
                    {
                        key: 'columns',
                        label: 'Columns to keep',
                        kind: 'columns',
                        required: true,
                        description:
                            'Selected columns flow through in the listed order; everything else is dropped.',
                    },
                ],
            },
        ],
    },

    'xf.map': {
        id: 'xf.map',
        kind: 'transform',
        label: 'Map',
        description:
            'Visual row mapper. Define each output column as an expression over the input row, with optional lookup inputs.',
        schemaSource: 'declared',
        sections: [
            {
                label: 'Mapping',
                fields: [
                    {
                        key: 'mode',
                        label: 'Mode',
                        kind: 'select',
                        defaultValue: 'expressions',
                        options: [
                            { label: 'Expressions', value: 'expressions' },
                            { label: 'Visual mapper', value: 'visual' },
                        ],
                    },
                    {
                        key: 'expressions',
                        label: 'Output expressions',
                        kind: 'key-value',
                        description:
                            'key = output column name, value = SQL expression. Example: total_with_tax → amount * 1.08',
                    },
                ],
            },
        ],
    },

    'xf.groupby': {
        id: 'xf.groupby',
        kind: 'transform',
        label: 'Group By',
        description: 'Group rows by key columns and apply aggregations.',
        schemaSource: 'declared',
        sections: [
            {
                label: 'Grouping',
                fields: [
                    {
                        key: 'groupKeys',
                        label: 'Group by columns',
                        kind: 'columns',
                        required: true,
                        description: 'Rows with the same values in these columns are grouped.',
                    },
                ],
            },
            {
                label: 'Aggregations',
                fields: [
                    {
                        key: 'aggregations',
                        label: 'Aggregations',
                        kind: 'aggregations',
                        required: true,
                    },
                ],
            },
            {
                label: 'Output',
                fields: [
                    {
                        key: 'havingClause',
                        label: 'HAVING clause',
                        kind: 'expression',
                        rows: 2,
                        placeholder: 'sum_amount > 1000',
                        description: 'Optional filter applied to groups after aggregation.',
                    },
                ],
            },
        ],
    },

    'xf.sort': {
        id: 'xf.sort',
        kind: 'transform',
        label: 'Sort',
        schemaSource: 'upstream',
        sections: [
            {
                label: 'Sort',
                fields: [
                    {
                        key: 'sortColumn',
                        label: 'Column',
                        kind: 'column',
                        required: true,
                    },
                    {
                        key: 'direction',
                        label: 'Direction',
                        kind: 'select',
                        defaultValue: 'asc',
                        options: [
                            { label: 'Ascending', value: 'asc' },
                            { label: 'Descending', value: 'desc' },
                        ],
                    },
                    {
                        key: 'nullsLast',
                        label: 'NULLs last',
                        kind: 'bool',
                        defaultValue: true,
                    },
                ],
            },
        ],
    },

    'xf.distinct': {
        id: 'xf.distinct',
        kind: 'transform',
        label: 'Distinct',
        schemaSource: 'upstream',
        sections: [
            {
                label: 'Distinct',
                fields: [
                    {
                        key: 'columns',
                        label: 'Distinct columns',
                        kind: 'columns',
                        description:
                            'Leave empty to deduplicate on the whole row.',
                    },
                ],
            },
        ],
    },

    'snk.csv': {
        id: 'snk.csv',
        kind: 'sink',
        label: 'CSV',
        description: 'Write delimited text files.',
        schemaSource: 'upstream',
        sections: [
            {
                label: 'Destination file',
                fields: [
                    {
                        key: 'path',
                        label: 'Output path',
                        kind: 'save-path',
                        required: true,
                        filters: [
                            { name: 'CSV', extensions: ['csv'] },
                            { name: 'TSV', extensions: ['tsv'] },
                            { name: 'All files', extensions: ['*'] },
                        ],
                    },
                    {
                        key: 'mode',
                        label: 'Write mode',
                        kind: 'select',
                        defaultValue: 'overwrite',
                        options: [
                            { label: 'Overwrite (replace)', value: 'overwrite' },
                            { label: 'Error if exists', value: 'error' },
                        ],
                    },
                    {
                        key: 'delimiter',
                        label: 'Delimiter',
                        kind: 'select',
                        defaultValue: ',',
                        options: [
                            { label: 'Comma  ,', value: ',' },
                            { label: 'Tab  \\t', value: '\t' },
                            { label: 'Semicolon  ;', value: ';' },
                            { label: 'Pipe  |', value: '|' },
                        ],
                    },
                    {
                        key: 'writeHeader',
                        label: 'Write header row',
                        kind: 'bool',
                        defaultValue: true,
                    },
                    {
                        key: 'encoding',
                        label: 'Encoding',
                        kind: 'select',
                        defaultValue: 'utf-8',
                        options: [
                            { label: 'UTF-8', value: 'utf-8' },
                            { label: 'UTF-16', value: 'utf-16' },
                            { label: 'Latin-1', value: 'latin-1' },
                        ],
                    },
                ],
            },
        ],
    },

    'snk.s3': {
        id: 'snk.s3',
        kind: 'sink',
        label: 'Amazon S3',
        description: 'Write CSV / Parquet / JSON to an s3:// URI via DuckDB httpfs.',
        schemaSource: 'upstream',
        sections: [
            {
                label: 'Destination',
                fields: [
                    {
                        key: 'path',
                        label: 'S3 URI',
                        kind: 'text',
                        required: true,
                        placeholder: 's3://bucket/path/out.parquet',
                        description: 'Full S3 URI. Format is inferred from the extension.',
                    },
                    {
                        key: 'connectionRef',
                        label: 'Or use saved connection',
                        kind: 'connection-ref',
                        accepts: ['s3'],
                    },
                    {
                        key: 'format',
                        label: 'Format override',
                        kind: 'select',
                        options: [
                            { label: 'Auto-detect from extension', value: '' },
                            { label: 'CSV', value: 'csv' },
                            { label: 'Parquet', value: 'parquet' },
                            { label: 'JSON', value: 'json' },
                        ],
                    },
                ],
            },
            {
                label: 'Credentials',
                fields: [
                    { key: 'accessKey', label: 'Access key', kind: 'text' },
                    { key: 'secretKey', label: 'Secret key', kind: 'text', placeholder: '••••••••' },
                    { key: 'region', label: 'Region', kind: 'text', placeholder: 'us-east-1' },
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
        ],
        ports: { inputs: [{ id: 'main', label: 'in', type: 'main' }], outputs: [] },
    },

    'snk.parquet': {
        id: 'snk.parquet',
        kind: 'sink',
        label: 'Parquet',
        description: 'Write columnar Parquet files.',
        schemaSource: 'upstream',
        sections: [
            {
                label: 'Destination file',
                fields: [
                    {
                        key: 'path',
                        label: 'Output path',
                        kind: 'save-path',
                        required: true,
                        filters: [
                            { name: 'Parquet', extensions: ['parquet'] },
                            { name: 'All files', extensions: ['*'] },
                        ],
                    },
                    {
                        key: 'mode',
                        label: 'Write mode',
                        kind: 'select',
                        defaultValue: 'overwrite',
                        options: [
                            { label: 'Overwrite', value: 'overwrite' },
                            { label: 'Append', value: 'append' },
                            { label: 'Error if exists', value: 'error' },
                        ],
                    },
                    {
                        key: 'compression',
                        label: 'Compression',
                        kind: 'select',
                        defaultValue: 'snappy',
                        options: [
                            { label: 'Snappy (fast)', value: 'snappy' },
                            { label: 'Zstd (smaller)', value: 'zstd' },
                            { label: 'Gzip', value: 'gzip' },
                            { label: 'LZ4', value: 'lz4' },
                            { label: 'None', value: 'none' },
                        ],
                    },
                    {
                        key: 'rowGroupSize',
                        label: 'Row group size',
                        kind: 'integer',
                        defaultValue: 100000,
                        description: 'Number of rows per row group.',
                    },
                    {
                        key: 'partitionBy',
                        label: 'Partition by columns',
                        kind: 'columns',
                        description: 'Write Hive-style partitioned directories per value.',
                    },
                    {
                        key: 'maxPartitions',
                        label: 'Max partitions',
                        kind: 'integer',
                        defaultValue: 10000,
                        description: 'Safety cap: abort before writing if partitioning would create more than this many files (one per distinct value). 0 = unlimited. Only applies when Partition by columns is set.',
                    },
                ],
            },
        ],
    },

    'snk.sqlite': {
        id: 'snk.sqlite',
        kind: 'sink',
        label: 'SQLite',
        description: 'Write to a SQLite database file.',
        schemaSource: 'upstream',
        sections: [
            {
                label: 'Destination',
                fields: [
                    {
                        key: 'database',
                        label: 'Database file',
                        kind: 'save-path',
                        required: true,
                        filters: [
                            { name: 'SQLite', extensions: ['db', 'sqlite', 'sqlite3'] },
                            { name: 'All files', extensions: ['*'] },
                        ],
                    },
                    {
                        key: 'tableName',
                        label: 'Table name',
                        kind: 'text',
                        required: true,
                        placeholder: 'orders',
                    },
                    {
                        key: 'mode',
                        label: 'Write mode',
                        kind: 'select',
                        defaultValue: 'overwrite',
                        options: [
                            { label: 'Create or replace', value: 'overwrite' },
                            { label: 'Append (insert)', value: 'append' },
                            { label: 'Truncate + insert', value: 'truncate' },
                            { label: 'Upsert (delete-by-key + re-insert)', value: 'upsert' },
                            { label: 'Merge (update only provided columns)', value: 'merge' },
                        ],
                        description: 'Upsert deletes rows matching the conflict columns, then re-inserts (issue #19). Merge updates only the columns the source provides and inserts new rows, leaving other target columns untouched (issue #39).',
                    },
                    {
                        key: 'conflictColumns',
                        label: 'Conflict columns (upsert key)',
                        kind: 'columns',
                        description: 'Required in Upsert mode: rows matching these key columns are replaced (set-based delete + re-insert), the rest inserted.',
                    },
                    {
                        key: 'deleteColumn',
                        label: 'Delete flag column (optional)',
                        kind: 'text',
                        placeholder: '_change_type',
                        description: 'Upsert only: rows whose value here equals the Delete value are removed from the target by key instead of upserted. Wire a CDC Diff / DuckLake change-type column here to propagate deletes.',
                    },
                    {
                        key: 'deleteValue',
                        label: 'Delete flag value',
                        kind: 'text',
                        defaultValue: 'delete',
                        description: 'The value in the Delete flag column that marks a row for deletion.',
                    },
                ],
            },
        ],
    },

    'snk.duckdb': {
        id: 'snk.duckdb',
        kind: 'sink',
        label: 'DuckDB',
        description: 'Write to a DuckDB database file.',
        schemaSource: 'upstream',
        sections: [
            {
                label: 'Destination',
                fields: [
                    {
                        key: 'database',
                        label: 'Database file',
                        kind: 'save-path',
                        required: true,
                        filters: [
                            { name: 'DuckDB', extensions: ['duckdb', 'db'] },
                            { name: 'All files', extensions: ['*'] },
                        ],
                    },
                    {
                        key: 'tableName',
                        label: 'Table name',
                        kind: 'text',
                        required: true,
                    },
                    {
                        key: 'mode',
                        label: 'Write mode',
                        kind: 'select',
                        defaultValue: 'overwrite',
                        options: [
                            { label: 'Create or replace', value: 'overwrite' },
                            { label: 'Append (insert)', value: 'append' },
                            { label: 'Truncate + insert', value: 'truncate' },
                            { label: 'Upsert (delete-by-key + re-insert)', value: 'upsert' },
                            { label: 'Merge (update only provided columns)', value: 'merge' },
                        ],
                        description: 'Upsert deletes rows matching the conflict columns, then re-inserts (issue #19). Merge updates only the columns the source provides and inserts new rows, leaving other target columns untouched (issue #39).',
                    },
                    {
                        key: 'conflictColumns',
                        label: 'Conflict columns (upsert key)',
                        kind: 'columns',
                        description: 'Required in Upsert mode: rows matching these key columns are replaced (set-based delete + re-insert), the rest inserted.',
                    },
                    {
                        key: 'deleteColumn',
                        label: 'Delete flag column (optional)',
                        kind: 'text',
                        placeholder: '_change_type',
                        description: 'Upsert only: rows whose value here equals the Delete value are removed from the target by key instead of upserted. Wire a CDC Diff / DuckLake change-type column here to propagate deletes.',
                    },
                    {
                        key: 'deleteValue',
                        label: 'Delete flag value',
                        kind: 'text',
                        defaultValue: 'delete',
                        description: 'The value in the Delete flag column that marks a row for deletion.',
                    },
                ],
            },
        ],
    },
};

export function getManifest(componentId: string | undefined): ComponentManifest | undefined {
    if (!componentId) return undefined;
    const m = MANIFESTS[componentId] ?? synthesizeManifest(componentId);
    if (m && !m.ports) {
        for (const cat of PALETTE) {
            for (const grp of cat.groups) {
                for (const c of grp.components) {
                    if (c.id === componentId) {
                        return { ...m, ports: portsForComponent(c) };
                    }
                }
            }
        }
    }
    return m;
}

export function getDefaults(manifest: ComponentManifest): Record<string, unknown> {
    const defaults: Record<string, unknown> = {};
    for (const section of manifest.sections) {
        for (const field of section.fields) {
            if (field.defaultValue !== undefined) {
                defaults[field.key] = field.defaultValue;
            }
        }
    }
    return defaults;
}
