import type { ComponentManifest, AutodetectFn } from './types';
import type { Column } from '../../pipeline-types';
import { synthesizeManifest, portsForComponent } from './manifest-synth';
import { getExternalManifest, PALETTE } from '../palette-data';
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
        // Always ask the real Rust engine on the desktop; it decides what the
        // location is per connector. tauriAutodetect throws on a desktop engine
        // failure (surfaced by the caller) and returns null only in the web
        // editor, where we show the illustrative sample below. Gating on a
        // fixed set of location names silently excluded connectors (issue #148).
        const real = await tauriAutodetect(format, props);
        if (real) return { columns: real.columns, sampleRows: real.sampleRows };
        await new Promise(r => setTimeout(r, 250));
        return { columns: mockColumns, sampleRows: mockRows };
    };
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
            {
                // The engine has read these since #98 and this hand-written
                // manifest never offered them, so the only way to reach a
                // ragged CSV was to edit the pipeline file by hand. The
                // synthesized manifest does have them - but a component listed
                // in MANIFESTS never reaches the synthesizer, which is exactly
                // how a hand-authored panel hides an engine feature.
                label: 'Malformed rows',
                fields: [
                    {
                        key: 'nullPadding',
                        label: 'Pad short rows with NULL',
                        kind: 'bool',
                        defaultValue: false,
                        description:
                            'A row with fewer columns than the header is padded with NULLs instead of failing the read. This is the one for "Expected Number of Columns: 5 Found: 4" when the row is real data with a trailing field missing. Maps to read_csv null_padding=true.',
                    },
                    {
                        key: 'ignoreErrors',
                        label: 'Skip rows that will not parse',
                        kind: 'bool',
                        defaultValue: false,
                        description:
                            'Drop any row DuckDB cannot read - bad encoding, wrong column count, a trailing blank line - instead of failing the whole file. It does not report which rows went, so prefer padding when the data is worth keeping. Maps to read_csv ignore_errors=true.',
                    },
                    {
                        key: 'readOptions',
                        label: 'Extra read options',
                        kind: 'key-value',
                        description:
                            'Any other DuckDB read_csv option, passed through as key=value (e.g. strict_mode=false, union_by_name=true, sample_size=-1). For a file that is ragged in a way the boxes above do not cover.',
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

    'src.teradata': {
        id: 'src.teradata',
        kind: 'source',
        label: 'Teradata',
        description:
            'Read from Teradata through its free ODBC driver (there is no DuckDB Teradata extension or native Rust driver). Install the Teradata ODBC driver on the machine that runs the pipeline, then connect with the friendly fields below, a DSN, or a full ODBC connection string. Numbers, decimals, dates and timestamps keep their types.',
        schemaSource: 'declared',
        sections: [
            {
                label: 'Connection',
                fields: [
                    {
                        key: 'driver',
                        label: 'ODBC driver name',
                        kind: 'text',
                        placeholder: 'Teradata Database ODBC Driver 17.20',
                        description: 'Name of the installed Teradata ODBC driver, as registered with the ODBC driver manager. Leave blank to use "Teradata Database ODBC Driver 17.20". Ignored when a DSN or connection string is set.',
                    },
                    { key: 'host', label: 'Host (DBCNAME)', kind: 'text', placeholder: 'teradata.example.com' },
                    { key: 'user', label: 'User', kind: 'text' },
                    { key: 'password', label: 'Password', kind: 'text', placeholder: '••••••••' },
                    { key: 'database', label: 'Default database (optional)', kind: 'text' },
                    {
                        key: 'dsn',
                        label: 'DSN (optional)',
                        kind: 'text',
                        description: 'Use a preconfigured ODBC Data Source Name instead of the driver + host fields. The user / password / database above still apply.',
                    },
                    {
                        key: 'connectionString',
                        label: 'ODBC connection string (optional)',
                        kind: 'text',
                        placeholder: 'DRIVER={Teradata Database ODBC Driver 17.20};DBCNAME=...;UID=...;PWD=...',
                        description: 'Full ODBC connection string. When set, it is used verbatim and every field above is ignored.',
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
                        rows: 4,
                        placeholder: 'SELECT * FROM sales.orders',
                        description: 'Leave blank to read the whole table named below.',
                    },
                    {
                        key: 'tableName',
                        label: 'Or table name',
                        kind: 'text',
                        placeholder: 'sales.orders',
                        description: 'Read this entire table when no SQL query is given.',
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

    'snk.teradata': {
        id: 'snk.teradata',
        kind: 'sink',
        label: 'Teradata',
        description:
            'Write to Teradata through its free ODBC driver. Install the Teradata ODBC driver on the machine that runs the pipeline. Append creates the table if missing then appends rows; Overwrite clears the table first. Rows are inserted one statement at a time (Teradata VALUES is single-row), so for very large loads use Teradata bulk utilities. Upsert is not supported.',
        schemaSource: 'upstream',
        sections: [
            {
                label: 'Connection',
                fields: [
                    {
                        key: 'driver',
                        label: 'ODBC driver name',
                        kind: 'text',
                        placeholder: 'Teradata Database ODBC Driver 17.20',
                        description: 'Name of the installed Teradata ODBC driver, as registered with the ODBC driver manager. Leave blank to use "Teradata Database ODBC Driver 17.20". Ignored when a DSN or connection string is set.',
                    },
                    { key: 'host', label: 'Host (DBCNAME)', kind: 'text', placeholder: 'teradata.example.com' },
                    { key: 'user', label: 'User', kind: 'text' },
                    { key: 'password', label: 'Password', kind: 'text', placeholder: '••••••••' },
                    {
                        key: 'dsn',
                        label: 'DSN (optional)',
                        kind: 'text',
                        description: 'Use a preconfigured ODBC Data Source Name instead of the driver + host fields. The user / password above still apply.',
                    },
                    {
                        key: 'connectionString',
                        label: 'ODBC connection string (optional)',
                        kind: 'text',
                        placeholder: 'DRIVER={Teradata Database ODBC Driver 17.20};DBCNAME=...;UID=...;PWD=...',
                        description: 'Full ODBC connection string. When set, it is used verbatim and every field above is ignored.',
                    },
                ],
            },
            {
                label: 'Destination',
                fields: [
                    {
                        key: 'tableName',
                        label: 'Target table',
                        kind: 'text',
                        required: true,
                        placeholder: 'orders_loaded',
                    },
                    {
                        key: 'database',
                        label: 'Target database (optional)',
                        kind: 'text',
                        description: 'Database the table lives in. Qualifies the table name and sets the connection default database. Leave blank to use the connection default.',
                    },
                    {
                        key: 'writeMode',
                        label: 'Write mode',
                        kind: 'select',
                        defaultValue: 'append',
                        options: [
                            { label: 'Append (create table if missing)', value: 'append' },
                            { label: 'Overwrite (clear table first)', value: 'overwrite' },
                        ],
                    },
                ],
            },
        ],
        ports: {
            inputs: [{ id: 'main', label: 'main', type: 'main' }],
            outputs: [],
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
            // Symmetric with snk.s3, which has always offered these. The
            // engine's S3 secret needs accessKey and secretKey - without them
            // no CREATE SECRET is emitted at all - and region silently defaults
            // to us-east-1, so a bucket anywhere else was signed for the wrong
            // region with nothing in the panel to correct it. A saved connection
            // was the only way in.
            {
                label: 'Credentials',
                fields: [
                    { key: 'accessKey', label: 'Access key', kind: 'text' },
                    { key: 'secretKey', label: 'Secret key', kind: 'text', placeholder: '••••••••' },
                    { key: 'region', label: 'Region', kind: 'text', placeholder: 'us-east-1' },
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
                        // Not required: custom-SQL mode runs standalone. The
                        // engine returns an empty ATTACH prelude when database
                        // is unset (attach_prelude), and build_duckdb_source
                        // wraps a custom `sql` without referencing an attached
                        // database at all. Marking it required blocked a
                        // pipeline in the GUI that the CLI ran fine (#201).
                        // Only table-name mode actually needs it.
                        required: false,
                        description:
                            'Required when reading a table by name. Custom SQL that does not read from an attached database can leave this empty.',
                        kind: 'file-path',
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
                        description:
                            'Expand nested objects into their own columns. With a records path set the records are always expanded, and this says whether nesting INSIDE them is expanded too (on unless you turn it off).',
                    },
                    {
                        key: 'keepParentNames',
                        label: 'Keep parent names',
                        kind: 'bool',
                        defaultValue: false,
                        description:
                            'Name a flattened column after the object it came from: owner.Id and account.Id rather than Id_1 and Id_2. Useful when the same key repeats at several levels.',
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
                    {
                        // The builder has read this since the audit that added
                        // it; the form never offered it, so the cheap
                        // deterministic path was unreachable and every run
                        // paid for ORDER BY ALL instead.
                        key: 'orderBy',
                        label: 'Tie-break columns (optional)',
                        kind: 'columns',
                        description:
                            'Which row survives each duplicate group. Only used when Distinct columns is set. Left empty, the whole row is sorted so the result stays the same run to run, which is correct but costs a full sort on every column. Naming a few columns here keeps that determinism far more cheaply.',
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
                    {
                        // build_csv_sink emits NULLSTR from this
                        // (builders.rs:9170) and the panel never offered it, so
                        // an empty cell and a NULL were indistinguishable in
                        // every file Duckle wrote.
                        key: 'nullValue',
                        label: 'Write NULL as',
                        kind: 'text',
                        placeholder: 'leave blank for an empty field',
                        description:
                            'The text written for a NULL. Blank (the default) writes nothing between the delimiters, which a reader cannot tell from an empty string. Common choices are \\N and NULL. Maps to COPY ... (NULLSTR).',
                    },
                ],
            },
            {
                // builders.rs:9179 reads partitionBy for the CSV sink exactly as
                // it does for Parquet, and the synthesized manifest offers it -
                // but snk.csv is hand-written in MANIFESTS, so it never reaches
                // the synthesizer. Same shape of gap as the src.csv one below.
                label: 'Partitioning',
                fields: [
                    {
                        key: 'partitionBy',
                        label: 'Partition by columns',
                        kind: 'columns',
                        description:
                            'Write a Hive-style partitioned dataset under the output path instead of one file. Each column becomes a directory level (col=value/). Reruns overwrite the slice just emitted and leave sibling partitions alone.',
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
                // build_cloud_sink delegates to the SAME builders as the local
                // file sinks (builders.rs:9086-9093), so every one of these is
                // already honoured on an s3:// write - the panel simply never
                // offered them, which left ZSTD Parquet and comma-with-header
                // CSV as the only things this sink could produce.
                //
                // partitionBy is deliberately absent: builders.rs:9084 strips it
                // before delegating, so a control for it would do nothing.
                label: 'Write options',
                fields: [
                    {
                        key: 'compression',
                        label: 'Compression (Parquet)',
                        kind: 'select',
                        defaultValue: 'zstd',
                        options: [
                            { label: 'Zstd (smaller)', value: 'zstd' },
                            { label: 'Snappy (fast)', value: 'snappy' },
                            { label: 'Gzip', value: 'gzip' },
                            { label: 'LZ4', value: 'lz4' },
                            { label: 'None', value: 'none' },
                        ],
                    },
                    {
                        key: 'compressionLevel',
                        label: 'Compression level (Parquet)',
                        kind: 'integer',
                        visibleWhen: { key: 'compression', equals: 'zstd' },
                        description: 'ZSTD only (1-22). Leave empty for DuckDB\'s default.',
                    },
                    {
                        key: 'parquetVersion',
                        label: 'Parquet version',
                        kind: 'select',
                        defaultValue: 'v1',
                        options: [
                            { label: 'V1 (maximum compatibility)', value: 'v1' },
                            { label: 'V2 (newer encodings)', value: 'v2' },
                        ],
                    },
                    {
                        key: 'rowGroupSize',
                        label: 'Row group size (Parquet)',
                        kind: 'integer',
                        description: 'Rows per row group. Leave empty for DuckDB\'s default (~122,880); a larger value cuts metadata overhead on big writes.',
                    },
                    {
                        key: 'delimiter',
                        label: 'Delimiter (CSV)',
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
                        label: 'Write header row (CSV)',
                        kind: 'bool',
                        defaultValue: true,
                    },
                    {
                        key: 'nullValue',
                        label: 'Write NULL as (CSV)',
                        kind: 'text',
                        placeholder: 'leave blank for an empty field',
                        description: 'The text written for a NULL, e.g. \\N. Blank writes nothing between the delimiters.',
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
                        key: 'compressionLevel',
                        label: 'Compression level',
                        kind: 'integer',
                        visibleWhen: { key: 'compression', equals: 'zstd' },
                        description: 'ZSTD only (1-22). Leave empty to let DuckDB pick its default. Higher = smaller files, slower writes.',
                    },
                    {
                        key: 'parquetVersion',
                        label: 'Parquet version',
                        kind: 'select',
                        defaultValue: 'v1',
                        options: [
                            { label: 'V1 (maximum compatibility)', value: 'v1' },
                            { label: 'V2 (newer encodings)', value: 'v2' },
                        ],
                        description: 'V1 is the most widely compatible. V2 enables newer Parquet encodings for smaller files.',
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
                    {
                        // #319. One field rather than a checkbox plus a column:
                        // a checkbox ticked with no column chosen is a state
                        // the engine would have to guess at, and guessing which
                        // column holds the geometry is how the wrong one gets
                        // sorted on.
                        key: 'hilbertColumn',
                        label: 'Spatial sort (Hilbert)',
                        kind: 'text',
                        placeholder: 'geometry column, e.g. geom',
                        description: 'Name a GEOMETRY column to sort rows along a Hilbert curve before writing, so geometries that are close on the ground land in the same row group and a spatial filter can skip more of the file. The curve is scaled to this dataset’s own extent, which costs one extra pass over the data. Leave empty to write rows in the order they arrive.',
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
    // #307: an external component's form comes from its own manifest. Checked
    // before the built-ins rather than after, so a workspace's component is
    // described by what it declares rather than by a synthesized guess from
    // its id - and a palette tile with no editable properties is not a tile
    // worth having.
    if (componentId.startsWith('ext.')) {
        const declared = getExternalManifest(componentId) as ComponentManifest | undefined;
        if (declared) return declared;
    }
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
