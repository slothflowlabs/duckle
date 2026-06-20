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
        // If we know the format and the node has a location set, try the real
        // Rust autodetect command via Tauri; fall back to a placeholder.
        // Different connectors carry "where to look" under different keys
        // (file path, DuckDB/DuckLake database/catalog, a URL, or a host), so
        // accept any of them - keying only on `path` skipped embedded and
        // network sources entirely (issue #18).
        if (format) {
            const stringy = (v: unknown) => typeof v === 'string' && v.trim().length > 0;
            const hasLocation =
                stringy(props.path) ||
                stringy(props.database) ||
                stringy(props.catalog) ||
                stringy(props.url) ||
                stringy(props.host);
            if (hasLocation) {
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

// Write-mode + conflict-columns for driver DB sinks that support MERGE upsert
// (SQL Server, Oracle, Snowflake). Upsert MERGEs on the conflict columns.
const upsertModeFields = (supportsMerge = false): Field[] => [
    {
        key: 'mode',
        label: 'Write mode',
        kind: 'select',
        defaultValue: 'overwrite',
        options: [
            { label: 'Overwrite (create / append)', value: 'overwrite' },
            { label: 'Append (insert)', value: 'append' },
            { label: 'Upsert (MERGE on key)', value: 'upsert' },
            // Merge is only offered for DuckDB-native targets (issue #39).
            ...(supportsMerge
                ? [{ label: 'Merge (update only provided columns)', value: 'merge' }]
                : []),
        ],
        description: supportsMerge
            ? 'Upsert replaces whole rows (delete-by-key + re-insert). Merge updates only the columns the source provides and inserts new rows, leaving other target columns untouched (issue #39).'
            : 'Upsert runs a MERGE: update rows that match the conflict columns, insert the rest.',
    },
    {
        key: 'conflictColumns',
        label: 'Conflict columns (upsert key)',
        kind: 'columns',
        description: 'Key columns to match on for Upsert / MERGE. Required when Write mode is Upsert.',
    },
    {
        key: 'deleteColumn',
        label: 'Delete flag column (optional)',
        kind: 'text',
        placeholder: '_change_type',
        description:
            'Upsert only: rows whose value in this column equals the Delete value are removed from the target by key instead of upserted. Wire a CDC Diff / DuckLake CDC change-type column here to propagate deletes.',
    },
    {
        key: 'deleteValue',
        label: 'Delete flag value',
        kind: 'text',
        defaultValue: 'delete',
        description: 'The value in the delete flag column that marks a row for deletion (default "delete").',
    },
];

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

// Map a database component to the saved-connection kind its picker should
// offer. Wire-compatible engines reuse a base kind (Cockroach speaks the
// Postgres protocol; OpenSearch speaks the Elasticsearch API). Components not
// listed get an unfiltered picker (any saved connection).
const CONNECTION_KIND_FOR: Record<string, string> = {
    'src.postgres': 'postgres', 'snk.postgres': 'postgres',
    'src.cockroach': 'postgres', 'snk.cockroach': 'postgres',
    'src.redshift': 'redshift', 'snk.redshift': 'redshift',
    'src.mysql': 'mysql', 'snk.mysql': 'mysql',
    'src.mariadb': 'mariadb', 'snk.mariadb': 'mariadb',
    'src.sqlserver': 'sqlserver', 'snk.sqlserver': 'sqlserver',
    'src.oracle': 'oracle', 'snk.oracle': 'oracle',
    'src.clickhouse': 'clickhouse', 'snk.clickhouse': 'clickhouse',
    'src.mongodb': 'mongodb', 'snk.mongodb': 'mongodb',
    'src.redis': 'redis', 'snk.redis': 'redis',
    'src.elastic': 'elastic', 'snk.elastic': 'elastic',
    'src.opensearch': 'elastic', 'snk.opensearch': 'elastic',
    'src.kafka': 'kafka', 'snk.kafka': 'kafka',
};

// "Pick a saved connection" dropdown. Placed at the TOP of a credential block
// so it reads as the primary way to fill the fields below (issue #30).
// `acceptKind` filters the list to compatible saved connections.
const connectionRefField = (acceptKind?: string): Field => ({
    key: 'connectionRef',
    label: 'Saved connection',
    kind: 'connection-ref',
    accepts: acceptKind ? [acceptKind] : undefined,
    description: 'Pick a connection from the Connections folder to auto-fill the fields below.',
});

const credentialFields = (acceptKind?: string): Field[] => [
    connectionRefField(acceptKind),
    { key: 'username', label: 'Username', kind: 'text' },
    { key: 'password', label: 'Password', kind: 'text', placeholder: '••••••••' },
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
    connectionRefField(CONNECTION_KIND_FOR[componentId]),
    { key: 'host', label: 'Host', kind: 'text', required: true, placeholder: 'localhost' },
    {
        key: 'port',
        label: 'Port',
        kind: 'integer',
        defaultValue: DB_PORTS[componentId] ?? 0,
    },
    { key: 'database', label: 'Database', kind: 'text', required: true, placeholder: 'mydb' },
    { key: 'username', label: 'Username', kind: 'text' },
    { key: 'password', label: 'Password', kind: 'text', placeholder: '••••••••' },
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

// Read-mode fields shared by the ATTACH-backed duck sources (ducklake,
// motherduck, quack): read a whole table OR run a custom SQL query against the
// attached catalog. The engine's build_relational_source already honors
// mode=sql; this exposes the choice in the UI so all duck sources match
// src.duckdb's flexibility (issue #77).
const duckReadFields = (): Field[] => [
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
    { key: 'schemaName', label: 'Schema', kind: 'text', defaultValue: 'main' },
    {
        key: 'tableName',
        label: 'Table',
        kind: 'text',
        placeholder: 'orders',
        description: 'Used when Read mode is Whole table.',
    },
    {
        key: 'sql',
        label: 'SQL query',
        kind: 'expression',
        rows: 5,
        placeholder: 'SELECT * FROM duckle_src.main.orders WHERE status = $1',
        description: 'Used when Read mode is Custom SQL. Reference the attached source as duckle_src.',
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
        description: 'Used in upsert mode: Postgres / Cockroach use these as ON CONFLICT keys; MySQL / MariaDB rely on the target table\'s existing UNIQUE / PRIMARY KEY index; DuckDB / SQLite match on these for a set-based delete + re-insert.',
    },
    {
        key: 'deleteColumn',
        label: 'Delete flag column (optional)',
        kind: 'text',
        placeholder: '_change_type',
        description:
            'Upsert only: rows whose value in this column equals the Delete value are removed from the target by key instead of upserted. Wire a CDC Diff / DuckLake CDC change-type column here to propagate deletes.',
    },
    {
        key: 'deleteValue',
        label: 'Delete flag value',
        kind: 'text',
        defaultValue: 'delete',
        description: 'The value in the delete flag column that marks a row for deletion (default "delete").',
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

    // Run Job - optional upstream, pass-through out so several Run Job nodes
    // can be chained into a Master Job.
    if (id === 'ctl.runjob') {
        return {
            inputs: [{ id: 'main', label: 'main', type: 'main', optional: true }],
            outputs: [MAIN_OUT],
        };
    }

    // Parallelize - one input, multiple branch outputs that run concurrently;
    // each branch is an independent downstream subgraph.
    if (id === 'ctl.parallelize') {
        return {
            inputs: [MAIN_IN],
            outputs: [
                { id: 'main_1', label: 'branch 1', type: 'main' },
                { id: 'main_2', label: 'branch 2', type: 'main' },
                { id: 'main_3', label: 'branch 3', type: 'main', optional: true },
                { id: 'main_4', label: 'branch 4', type: 'main', optional: true },
            ],
        };
    }

    // Log / Warn - pass-through diagnostic: one input, one output.
    if (id === 'ctl.log' || id === 'ctl.warn') {
        return { inputs: [MAIN_IN], outputs: [MAIN_OUT] };
    }

    // Die - one input; an optional output so a non-firing Die can still
    // chain downstream (it passes rows through when its condition is false).
    if (id === 'ctl.die') {
        return {
            inputs: [MAIN_IN],
            outputs: [{ id: 'main', label: 'main', type: 'main', optional: true }],
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

    // Incremental load - simple pass-through filter: one input, one output.
    if (id === 'xf.incremental') {
        return { inputs: [MAIN_IN], outputs: [MAIN_OUT] };
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
    // The component id rarely matches the on-disk extension (src.excel ->
    // .xlsx, not ".excel"; issue #18). Map the known mismatches; everything
    // else uses the id suffix as before.
    const EXT_OVERRIDES: Record<string, string[]> = {
        'src.excel': ['xlsx', 'xls'],
    };
    // src.spatial reads many geo formats via GDAL; surface the common
    // ones in the file picker rather than a useless ".spatial" filter.
    const filters = comp.id === 'src.spatial'
        ? [
            { name: 'Geospatial', extensions: ['geojson', 'json', 'shp', 'gpkg', 'kml', 'gpx', 'gml'] },
            { name: 'All files', extensions: ['*'] },
        ]
        : [
            { name: comp.label, extensions: EXT_OVERRIDES[comp.id] ?? [ext] },
            { name: 'All files', extensions: ['*'] },
        ];
    return base(comp, [
        {
            label: 'Source file',
            fields: [
                {
                    key: 'path',
                    label: 'Path',
                    kind: 'file-path',
                    required: true,
                    filters,
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

function partitionBySection(): FormSection {
    return {
        label: 'Partitioning (write side, applies to sinks only)',
        fields: [
            {
                key: 'partitionBy',
                label: 'Partition by columns',
                kind: 'columns',
                description: 'Write a Hive-style partitioned dataset under the output path. Each partition column becomes a directory level (col=value/). Reruns overwrite the slice we just emitted.',
            },
        ],
    };
}

function synthFileSink(comp: ComponentDef): ComponentManifest {
    if (comp.id === 'snk.ftp') {
        // File-transfer sink (write-side mirror of src.ftp). The view is
        // written to a local temp file in `format`, then uploaded to
        // `remotePath` over FTP / FTPS / SFTP.
        return base(comp, [
            {
                label: 'Connection',
                fields: [
                    { key: 'protocol', label: 'Protocol', kind: 'select', defaultValue: 'sftp',
                      options: [{label:'SFTP',value:'sftp'},{label:'FTP',value:'ftp'},{label:'FTPS',value:'ftps'}] },
                    { key: 'host', label: 'Host', kind: 'text', required: true },
                    { key: 'port', label: 'Port', kind: 'integer', defaultValue: 22,
                      description: 'SFTP: 22. FTP / FTPS: usually 21.' },
                    { key: 'user', label: 'Username', kind: 'text' },
                    { key: 'password', label: 'Password', kind: 'text', placeholder: '••••••••' },
                ],
            },
            {
                label: 'SFTP key auth (optional)',
                fields: [
                    { key: 'privateKey', label: 'Private key (PEM)', kind: 'text',
                      placeholder: '-----BEGIN OPENSSH PRIVATE KEY-----',
                      description: 'OpenSSH private key for SFTP key-based auth (instead of a password).' },
                    { key: 'keyPassphrase', label: 'Key passphrase', kind: 'text', placeholder: '••••••••' },
                    { key: 'hostFingerprint', label: 'Host fingerprint', kind: 'text',
                      placeholder: 'SHA256:...',
                      description: 'Optional SFTP host-key pin. If set, the connection is refused unless the server key matches this SHA256 fingerprint.' },
                ],
            },
            {
                label: 'Upload',
                fields: [
                    { key: 'remotePath', label: 'Remote path', kind: 'text', required: true,
                      placeholder: '/out/orders.csv',
                      description: 'Full remote path including the filename.' },
                    { key: 'format', label: 'Format', kind: 'select', defaultValue: 'csv',
                      options: [
                          { label: 'CSV', value: 'csv' },
                          { label: 'Parquet', value: 'parquet' },
                          { label: 'JSON', value: 'json' },
                          { label: 'JSONL / NDJSON', value: 'jsonl' },
                      ] },
                ],
            },
        ], 'upstream');
    }
    if (comp.id === 'snk.spatial') {
        // Geospatial sink writes via GDAL; the driver picks the actual
        // file format (GeoJSON / GeoPackage / Shapefile / KML / GPX).
        return base(comp, [
            {
                label: 'Destination file',
                fields: [
                    {
                        key: 'path',
                        label: 'Output path',
                        kind: 'save-path',
                        required: true,
                        filters: [
                            { name: 'Geospatial', extensions: ['geojson', 'gpkg', 'shp', 'kml', 'gpx'] },
                            { name: 'All files', extensions: ['*'] },
                        ],
                    },
                    {
                        key: 'driver',
                        label: 'OGR driver',
                        kind: 'select',
                        defaultValue: 'GeoJSON',
                        options: [
                            { label: 'GeoJSON', value: 'GeoJSON' },
                            { label: 'GeoPackage (.gpkg)', value: 'GPKG' },
                            { label: 'ESRI Shapefile', value: 'ESRI Shapefile' },
                            { label: 'KML', value: 'KML' },
                            { label: 'GPX', value: 'GPX' },
                        ],
                    },
                ],
            },
        ], 'upstream');
    }
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
                            { name: comp.label, extensions: comp.id === 'snk.excel' ? ['xlsx'] : [ext] },
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
    if (id === 'snk.excel') {
        return [{
            label: 'Format',
            fields: [
                { key: 'hasHeader', label: 'Has header row', kind: 'bool', defaultValue: true },
            ],
        }];
    }
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
                    // Explicit date / timestamp format passed through to
                    // DuckDB's read_csv_auto. Most useful for dd/mm/yyyy
                    // which DuckDB would otherwise misparse as mm/dd/yyyy.
                    // Only applies to source side; sinks don't read.
                    ...(id.startsWith('src.') ? [
                        {
                            key: 'dateFormat',
                            label: 'Date format (optional)',
                            kind: 'text' as const,
                            placeholder: '%d/%m/%Y',
                            description: 'strptime tokens. Common: %d/%m/%Y, %m/%d/%Y, %Y-%m-%d. Leave empty for auto-detect.',
                        },
                        {
                            key: 'timestampFormat',
                            label: 'Timestamp format (optional)',
                            kind: 'text' as const,
                            placeholder: '%d/%m/%Y %H:%M:%S',
                            description: 'strptime tokens, same as date format plus %H:%M:%S. Leave empty for auto-detect.',
                        },
                    ] : []),
                ],
            },
            partitionBySection(),
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
                    {
                        key: 'recordsPath',
                        label: 'Records path',
                        kind: 'text',
                        placeholder: 'data   or   response.records',
                        description:
                            "Dotted key path to the array of records inside the JSON, for API-style responses where the rows live under a key (e.g. {\"data\":[...]} -> 'data', or {\"response\":{\"records\":[...]}} -> 'response.records'). Each record is unnested and nested fields are flattened into columns. Leave blank for a plain top-level array or JSON Lines.",
                    },
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
            partitionBySection(),
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
    if (comp.id === 'src.ducklake.changes') {
        // DuckLake change-data-feed: reads table_changes() incrementally,
        // tracking the consumed snapshot in workspace state.
        return base(comp, [
            {
                label: 'Catalog',
                fields: [
                    { key: 'path', label: 'Catalog path', kind: 'text', required: true, placeholder: '/var/lakes/catalog.ducklake', description: 'Path to the DuckLake catalog (a .ducklake file or metadata DB DSN).' },
                ],
            },
            {
                label: 'Change feed',
                fields: [
                    { key: 'schema', label: 'Schema', kind: 'text', defaultValue: 'main' },
                    { key: 'table', label: 'Table', kind: 'text', required: true, placeholder: 'orders' },
                    { key: 'insertsOnly', label: 'Inserts only', kind: 'bool', defaultValue: false, description: 'Keep only insert changes (drop updates/deletes).' },
                    { key: 'initialSnapshot', label: 'Initial snapshot (first run)', kind: 'integer', defaultValue: 0, description: 'Snapshot id to start from before any state is saved; 0 = from the beginning.' },
                ],
            },
        ]);
    }
    if (comp.id === 'src.ducklake.diff') {
        // DuckLake Data Diff: the change feed between two explicit snapshots of
        // one table. Pick the From / To snapshots with the Browse picker.
        return base(comp, [
            {
                label: 'Catalog',
                fields: [
                    { key: 'path', label: 'Catalog path', kind: 'text', required: true, placeholder: '/var/lakes/catalog.ducklake', description: 'Path to the DuckLake catalog (a .ducklake file or metadata DB DSN).' },
                ],
            },
            {
                label: 'Table',
                fields: [
                    { key: 'schema', label: 'Schema', kind: 'text', defaultValue: 'main' },
                    { key: 'table', label: 'Table', kind: 'text', required: true, placeholder: 'orders' },
                ],
            },
            {
                label: 'Snapshots to compare',
                fields: [
                    { key: 'fromVersion', label: 'From snapshot', kind: 'ducklake-snapshot', required: true, placeholder: 'e.g. 2', description: 'The earlier snapshot id. Click Browse to pick from the catalog.' },
                    { key: 'toVersion', label: 'To snapshot', kind: 'ducklake-snapshot', required: true, placeholder: 'e.g. 5', description: 'The later snapshot id. Click Browse to pick from the catalog.' },
                ],
            },
        ]);
    }
    if (comp.id === 'src.ducklake') {
        // DuckLake attaches a catalog (path) and then names a specific
        // table inside it.
        return base(comp, [
            {
                label: 'Catalog',
                fields: [
                    { key: 'path', label: 'Catalog path', kind: 'text', required: true, placeholder: '/var/lakes/catalog.duckdb', description: 'Path to the DuckLake catalog file (DuckDB-format).' },
                ],
            },
            {
                label: 'Read',
                fields: duckReadFields(),
            },
            {
                label: 'Time travel',
                fields: [
                    { key: 'asOfVersion', label: 'As of snapshot / version', kind: 'ducklake-snapshot', placeholder: 'e.g. 12', description: 'Read the table as of this DuckLake snapshot id (time travel). Click Browse to pick from the catalog. Leave empty for the latest.' },
                    { key: 'asOfTimestamp', label: 'As of timestamp', kind: 'text', placeholder: 'YYYY-MM-DD HH:MM:SS', description: 'Read the table as of this point in time. Used only when no version is set.' },
                ],
            },
        ]);
    }
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

function synthLakehouseSink(comp: ComponentDef): ComponentManifest {
    if (comp.id === 'snk.ducklake') {
        return base(comp, [
            {
                label: 'Catalog',
                fields: [
                    { key: 'path', label: 'Catalog path', kind: 'text', required: true, placeholder: '/var/lakes/catalog.duckdb' },
                ],
            },
            {
                label: 'Destination',
                fields: [
                    { key: 'schemaName', label: 'Schema', kind: 'text', defaultValue: 'main' },
                    { key: 'tableName', label: 'Table', kind: 'text', required: true, placeholder: 'orders' },
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
                        description: 'Upsert deletes rows matching the conflict columns, then re-inserts (issue #19). Merge updates only the columns the source provides, leaving other target columns untouched (issue #39).',
                    },
                    {
                        key: 'conflictColumns',
                        label: 'Conflict columns (upsert key)',
                        kind: 'columns',
                        description: 'Key columns to match on for Upsert. Required when Write mode is Upsert.',
                    },
                ],
            },
        ], 'upstream');
    }
    // Same shape as the source: a table-root path. The driver lives in
    // the component id (iceberg / delta).
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
                    description: 'The Iceberg table root: a local directory or an s3:// URL backed by a Connection.',
                },
            ],
        },
    ], 'upstream');
}

function synthDbSource(comp: ComponentDef): ComponentManifest {
    if (comp.id === 'src.oracle') {
        return base(comp, [
            {
                label: 'Oracle connection',
                fields: [
                    { key: 'connect', label: 'Easy Connect string', kind: 'text', required: true, placeholder: 'host:1521/SERVICE' },
                    { key: 'user', label: 'User', kind: 'text', required: true },
                    { key: 'password', label: 'Password', kind: 'text', placeholder: '••••••••' },
                ],
            },
            {
                label: 'Query',
                fields: [
                    { key: 'schema', label: 'Schema (optional)', kind: 'text' },
                    { key: 'tableName', label: 'Table (for SELECT *)', kind: 'text' },
                    { key: 'query', label: 'Or custom SQL', kind: 'expression', rows: 4, placeholder: 'SELECT * FROM ...' },
                ],
            },
            {
                label: 'Runtime requirement',
                fields: [
                    {
                        key: 'oracleRuntimeNote',
                        label: 'Heads-up',
                        kind: 'text',
                        description: 'Oracle support is built into Duckle. Users only need Oracle Instant Client (libclntsh.so / OCI.dll / libclntsh.dylib) on the library path at runtime. If it is missing the executor surfaces a clear loader error.',
                    },
                ],
            },
        ]);
    }
    if (comp.id === 'src.sqlserver' || comp.id === 'src.synapse') {
        const vendor = comp.id === 'src.sqlserver' ? 'SQL Server' : 'Azure Synapse';
        return base(comp, [
            {
                label: `${vendor} connection`,
                fields: [
                    { key: 'host', label: 'Host', kind: 'text', required: true, placeholder: 'mssql.example.com' },
                    { key: 'port', label: 'Port', kind: 'integer', defaultValue: 1433 },
                    { key: 'user', label: 'User', kind: 'text', required: true },
                    { key: 'password', label: 'Password', kind: 'text', placeholder: '••••••••' },
                    { key: 'database', label: 'Database', kind: 'text', required: true },
                    { key: 'trustCert', label: 'Trust TLS cert (dev / self-signed)', kind: 'bool', defaultValue: false },
                ],
            },
            {
                label: 'Query',
                fields: [
                    { key: 'schema', label: 'Schema', kind: 'text', defaultValue: 'dbo' },
                    { key: 'tableName', label: 'Table (for SELECT *)', kind: 'text', placeholder: 'orders' },
                    { key: 'query', label: 'Or custom SQL', kind: 'expression', rows: 4, placeholder: 'SELECT * FROM ...' },
                ],
            },
        ]);
    }
    if (comp.id === 'src.clickhouse') {
        return base(comp, [
            {
                label: 'ClickHouse',
                fields: [
                    { key: 'endpoint', label: 'Endpoint', kind: 'text', required: true, placeholder: 'http://localhost:8123' },
                    { key: 'user', label: 'User', kind: 'text', placeholder: 'default' },
                    { key: 'password', label: 'Password', kind: 'text', placeholder: '••••••••' },
                    { key: 'database', label: 'Database (optional)', kind: 'text' },
                    { key: 'tableName', label: 'Table (for SELECT *)', kind: 'text', placeholder: 'events' },
                    { key: 'query', label: 'Or custom SQL', kind: 'expression', rows: 4, placeholder: 'SELECT * FROM events WHERE ...' },
                ],
            },
        ]);
    }
    return base(comp, [
        { label: 'Connection', fields: dbConnectionFields(comp.id) },
        { label: 'Query', fields: dbReadFields() },
    ]);
}

function synthDbSink(comp: ComponentDef): ComponentManifest {
    // Embedded file databases (SQLite / DuckDB). These attach a local file as
    // duckle_dst and write a table into it - they have no host/account, so the
    // generic network-DB fallback below would show the wrong fields. The engine
    // (build_db_sink) supports overwrite / append / upsert here, so surface the
    // full upsert mode picker, not just overwrite (issue #19).
    if (comp.id === 'snk.sqlite' || comp.id === 'snk.duckdb') {
        const isSqlite = comp.id === 'snk.sqlite';
        return base(comp, [
            {
                label: 'Database file',
                fields: [
                    {
                        key: 'database',
                        label: 'Database file',
                        kind: 'save-path',
                        required: true,
                        filters: isSqlite
                            ? [
                                { name: 'SQLite', extensions: ['sqlite', 'db', 'sqlite3'] },
                                { name: 'All files', extensions: ['*'] },
                            ]
                            : [
                                { name: 'DuckDB', extensions: ['duckdb', 'db'] },
                                { name: 'All files', extensions: ['*'] },
                            ],
                    },
                ],
            },
            {
                label: 'Destination',
                fields: [
                    { key: 'tableName', label: 'Table', kind: 'text', required: true, placeholder: 'orders' },
                    ...upsertModeFields(true),
                ],
            },
        ], 'upstream');
    }
    if (comp.id === 'snk.oracle') {
        return base(comp, [
            {
                label: 'Oracle connection',
                fields: [
                    { key: 'connect', label: 'Easy Connect string', kind: 'text', required: true, placeholder: 'host:1521/SERVICE' },
                    { key: 'user', label: 'User', kind: 'text', required: true },
                    { key: 'password', label: 'Password', kind: 'text', placeholder: '••••••••' },
                ],
            },
            {
                label: 'Destination',
                fields: [
                    { key: 'schema', label: 'Schema', kind: 'text' },
                    { key: 'tableName', label: 'Table', kind: 'text', required: true },
                    { key: 'batchSize', label: 'Insert batch size', kind: 'integer', defaultValue: 1000 },
                    ...upsertModeFields(),
                ],
            },
            {
                label: 'Runtime requirement',
                fields: [
                    {
                        key: 'oracleRuntimeNote',
                        label: 'Heads-up',
                        kind: 'text',
                        description: 'Oracle support is built into Duckle. Users only need Oracle Instant Client (libclntsh.so / OCI.dll / libclntsh.dylib) on the library path at runtime. If it is missing the executor surfaces a clear loader error.',
                    },
                ],
            },
        ], 'upstream');
    }
    if (comp.id === 'snk.sqlserver' || comp.id === 'snk.synapse') {
        const vendor = comp.id === 'snk.sqlserver' ? 'SQL Server' : 'Azure Synapse';
        return base(comp, [
            {
                label: `${vendor} connection`,
                fields: [
                    { key: 'host', label: 'Host', kind: 'text', required: true, placeholder: 'mssql.example.com' },
                    { key: 'port', label: 'Port', kind: 'integer', defaultValue: 1433 },
                    { key: 'user', label: 'User', kind: 'text', required: true },
                    { key: 'password', label: 'Password', kind: 'text', placeholder: '••••••••' },
                    { key: 'database', label: 'Database', kind: 'text', required: true },
                    { key: 'trustCert', label: 'Trust TLS cert (dev / self-signed)', kind: 'bool', defaultValue: false },
                ],
            },
            {
                label: 'Destination',
                fields: [
                    { key: 'schema', label: 'Schema', kind: 'text', defaultValue: 'dbo' },
                    { key: 'tableName', label: 'Table', kind: 'text', required: true, placeholder: 'orders' },
                    { key: 'batchSize', label: 'Insert batch size (max 1000)', kind: 'integer', defaultValue: 1000 },
                    ...upsertModeFields(),
                ],
            },
        ], 'upstream');
    }
    if (comp.id === 'snk.clickhouse') {
        return base(comp, [
            {
                label: 'ClickHouse',
                fields: [
                    { key: 'endpoint', label: 'Endpoint', kind: 'text', required: true, placeholder: 'http://localhost:8123' },
                    { key: 'user', label: 'User', kind: 'text', placeholder: 'default' },
                    { key: 'password', label: 'Password', kind: 'text', placeholder: '••••••••' },
                    { key: 'database', label: 'Database', kind: 'text' },
                    { key: 'tableName', label: 'Table', kind: 'text', required: true, placeholder: 'events' },
                    { key: 'batchSize', label: 'Batch size', kind: 'integer', defaultValue: 10000 },
                ],
            },
        ], 'upstream');
    }
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
    if (comp.id === 'src.snowflake') {
        return base(comp, [
            {
                label: 'Snowflake account',
                fields: [
                    { key: 'account', label: 'Account identifier', kind: 'text', required: true, placeholder: 'xy12345.us-east-1' },
                    {
                        key: 'authType',
                        label: 'Auth type',
                        kind: 'select',
                        defaultValue: 'pat',
                        options: [
                            { label: 'Personal Access Token (Bearer)', value: 'pat' },
                            { label: 'JWT (key-pair, RS256)', value: 'jwt' },
                        ],
                    },
                    { key: 'pat', label: 'Personal Access Token (PAT mode)', kind: 'text', placeholder: '••••••••' },
                    { key: 'user', label: 'User (JWT mode)', kind: 'text', placeholder: 'MY_USER' },
                    { key: 'privateKeyPath', label: 'PEM private key path (JWT mode)', kind: 'file-path' },
                    { key: 'warehouse', label: 'Warehouse', kind: 'text', placeholder: 'compute_wh' },
                    { key: 'role', label: 'Role', kind: 'text', placeholder: 'analyst' },
                    { key: 'endpoint', label: 'SQL API endpoint (override)', kind: 'text', placeholder: 'https://<account>.snowflakecomputing.com/api/v2/statements' },
                ],
            },
            {
                label: 'Query',
                fields: [
                    { key: 'database', label: 'Database', kind: 'text', placeholder: 'MYDB' },
                    { key: 'schema', label: 'Schema', kind: 'text', placeholder: 'PUBLIC' },
                    { key: 'tableName', label: 'Table (for SELECT *)', kind: 'text', placeholder: 'orders' },
                    { key: 'query', label: 'Or custom SQL', kind: 'expression', rows: 4, placeholder: 'SELECT * FROM ...' },
                ],
            },
        ]);
    }
    if (comp.id === 'src.databricks') {
        return base(comp, [
            {
                label: 'Databricks workspace',
                fields: [
                    { key: 'workspace', label: 'Workspace host', kind: 'text', required: true, placeholder: 'dbc-xxxxxxxx.cloud.databricks.com' },
                    { key: 'pat', label: 'Personal Access Token', kind: 'text', required: true, placeholder: '••••••••' },
                    { key: 'warehouseId', label: 'SQL warehouse ID', kind: 'text', required: true, placeholder: '0a1b2c3d4e5f6g7h' },
                ],
            },
            {
                label: 'Query',
                fields: [
                    { key: 'catalog', label: 'Catalog', kind: 'text', placeholder: 'main' },
                    { key: 'schema', label: 'Schema', kind: 'text', placeholder: 'default' },
                    { key: 'tableName', label: 'Table (for SELECT *)', kind: 'text', placeholder: 'orders' },
                    { key: 'query', label: 'Or custom SQL', kind: 'expression', rows: 4, placeholder: 'SELECT * FROM ...' },
                    { key: 'waitTimeoutSeconds', label: 'Sync wait (seconds, max 50)', kind: 'integer', defaultValue: 30 },
                ],
            },
        ]);
    }
    if (comp.id === 'src.redshift') {
        // Redshift speaks the Postgres wire protocol; reuse the same
        // libpq-style connection form (host/port/db/user/password) as
        // src.postgres but with the Redshift default port (5439).
        return base(comp, [
            { label: 'Redshift connection', fields: dbConnectionFields(comp.id) },
            { label: 'Source table', fields: [
                { key: 'schemaName', label: 'Schema', kind: 'text', defaultValue: 'public' },
                { key: 'tableName', label: 'Table', kind: 'text', required: true, placeholder: 'orders' },
            ] },
        ]);
    }
    if (comp.id === 'src.bigquery') {
        return base(comp, [
            {
                label: 'BigQuery project',
                fields: [
                    { key: 'project', label: 'Project ID', kind: 'text', required: true, placeholder: 'my-gcp-project' },
                    { key: 'dataset', label: 'Default dataset', kind: 'text', placeholder: 'analytics' },
                ],
            },
            {
                label: 'Source table',
                fields: [
                    { key: 'schemaName', label: 'Dataset', kind: 'text', placeholder: 'analytics' },
                    { key: 'tableName', label: 'Table', kind: 'text', required: true, placeholder: 'events' },
                ],
            },
            {
                label: 'Auth',
                fields: [
                    {
                        key: 'credentialsPath',
                        label: 'Service account JSON path (optional)',
                        kind: 'file-path',
                        description: 'When empty the engine relies on the standard GCP credential discovery (GOOGLE_APPLICATION_CREDENTIALS / gcloud default).',
                    },
                ],
            },
        ]);
    }
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
                    ...duckReadFields(),
                ],
            },
        ]);
    }
    if (comp.id === 'src.quack') {
        // Quack (DuckDB May 2026 remote protocol). The server runs
        // quack_serve(...) on port 9494 by default; the client ATTACHes
        // the quack: URL with a SECRET carrying the token. Requires a
        // Quack-enabled DuckDB build on both sides.
        return base(comp, [
            {
                label: 'Connection',
                fields: [
                    { key: 'host', label: 'Host', kind: 'text', required: true, placeholder: 'duck.example.com' },
                    { key: 'port', label: 'Port', kind: 'integer', defaultValue: 9494, description: 'Default Quack port is 9494.' },
                    { key: 'token', label: 'Token', kind: 'text', placeholder: 'super_secret', description: 'Auth token; matches the value passed to quack_serve(token=...). Leave empty for unauthenticated test servers.' },
                ],
            },
            {
                label: 'Query',
                fields: duckReadFields(),
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
                ...credentialFields('snowflake'),
            ],
        },
        { label: 'Query', fields: dbReadFields() },
    ]);
}

function synthWarehouseSink(comp: ComponentDef): ComponentManifest {
    if (comp.id === 'snk.databricks') {
        return base(comp, [
            {
                label: 'Databricks workspace',
                fields: [
                    { key: 'workspace', label: 'Workspace host', kind: 'text', required: true, placeholder: 'dbc-xxxxxxxx.cloud.databricks.com' },
                    { key: 'pat', label: 'Personal Access Token', kind: 'text', required: true, placeholder: '••••••••' },
                    { key: 'warehouseId', label: 'SQL warehouse ID', kind: 'text', required: true, placeholder: '0a1b2c3d4e5f6g7h' },
                ],
            },
            {
                label: 'Destination',
                fields: [
                    { key: 'catalog', label: 'Catalog', kind: 'text', placeholder: 'main' },
                    { key: 'schema', label: 'Schema', kind: 'text', placeholder: 'default' },
                    { key: 'tableName', label: 'Table', kind: 'text', required: true, placeholder: 'orders' },
                    {
                        key: 'batchSize',
                        label: 'Insert batch size',
                        kind: 'integer',
                        defaultValue: 1000,
                        description: 'Rows per multi-row INSERT. Larger = fewer round-trips, but the API has a body-size limit.',
                    },
                    {
                        key: 'waitTimeoutSeconds',
                        label: 'Sync wait (seconds, max 50)',
                        kind: 'integer',
                        defaultValue: 30,
                        description: 'How long to wait for each statement to complete. After this the statement continues async server-side; the engine treats it as success.',
                    },
                    ...upsertModeFields(),
                ],
            },
        ], 'upstream');
    }
    if (comp.id === 'snk.snowflake') {
        return base(comp, [
            {
                label: 'Snowflake account',
                fields: [
                    { key: 'account', label: 'Account identifier', kind: 'text', required: true, placeholder: 'xy12345.us-east-1' },
                    {
                        key: 'authType',
                        label: 'Auth type',
                        kind: 'select',
                        defaultValue: 'pat',
                        options: [
                            { label: 'Personal Access Token (Bearer)', value: 'pat' },
                            { label: 'JWT (key-pair, RS256)', value: 'jwt' },
                        ],
                    },
                    { key: 'pat', label: 'Personal Access Token (PAT mode)', kind: 'text', placeholder: '••••••••', description: 'Required when Auth type is PAT.' },
                    { key: 'user', label: 'User (JWT mode)', kind: 'text', placeholder: 'MY_USER', description: 'Required when Auth type is JWT. Uppercased automatically.' },
                    { key: 'privateKeyPath', label: 'PEM private key path (JWT mode)', kind: 'file-path', description: 'Required when Auth type is JWT. Reads PKCS#8-encoded RSA private key from disk; the engine signs RS256 claims and computes the public-key fingerprint.' },
                    { key: 'warehouse', label: 'Warehouse', kind: 'text', placeholder: 'compute_wh' },
                    { key: 'role', label: 'Role', kind: 'text', placeholder: 'analyst' },
                ],
            },
            {
                label: 'Destination',
                fields: [
                    { key: 'database', label: 'Database', kind: 'text', required: true },
                    { key: 'schema', label: 'Schema', kind: 'text', defaultValue: 'PUBLIC' },
                    { key: 'tableName', label: 'Table', kind: 'text', required: true, placeholder: 'orders' },
                    {
                        key: 'batchSize',
                        label: 'Insert batch size',
                        kind: 'integer',
                        defaultValue: 1000,
                        description: 'Rows per multi-row INSERT. Larger = fewer round-trips, but the SQL API has a body-size limit (~16 MB).',
                    },
                    ...upsertModeFields(),
                ],
            },
        ], 'upstream');
    }
    if (comp.id === 'snk.redshift') {
        return base(comp, [
            { label: 'Redshift connection', fields: dbConnectionFields(comp.id) },
            { label: 'Destination', fields: [
                { key: 'schemaName', label: 'Schema', kind: 'text', defaultValue: 'public' },
                { key: 'tableName', label: 'Table', kind: 'text', required: true, placeholder: 'orders' },
                {
                    key: 'mode',
                    label: 'Write mode',
                    kind: 'select',
                    defaultValue: 'overwrite',
                    options: [
                        { label: 'Create or replace', value: 'overwrite' },
                        { label: 'Append (insert)', value: 'append' },
                        { label: 'Truncate + insert', value: 'truncate' },
                        { label: 'Upsert on conflict', value: 'upsert' },
                    ],
                },
                {
                    key: 'conflictColumns',
                    label: 'Conflict columns (for upsert)',
                    kind: 'columns',
                },
            ] },
        ], 'upstream');
    }
    if (comp.id === 'snk.bigquery') {
        return base(comp, [
            {
                label: 'BigQuery project',
                fields: [
                    { key: 'project', label: 'Project ID', kind: 'text', required: true },
                    { key: 'dataset', label: 'Default dataset', kind: 'text', placeholder: 'analytics' },
                ],
            },
            {
                label: 'Destination',
                fields: [
                    { key: 'schemaName', label: 'Dataset', kind: 'text', placeholder: 'analytics' },
                    { key: 'tableName', label: 'Table', kind: 'text', required: true, placeholder: 'events' },
                    {
                        key: 'mode',
                        label: 'Write mode',
                        kind: 'select',
                        defaultValue: 'overwrite',
                        options: [
                            { label: 'Create or replace', value: 'overwrite' },
                            { label: 'Append (insert)', value: 'append' },
                            { label: 'Truncate + insert', value: 'truncate' },
                        ],
                    },
                ],
            },
            {
                label: 'Auth',
                fields: [
                    {
                        key: 'credentialsPath',
                        label: 'Service account JSON path (optional)',
                        kind: 'file-path',
                        description: 'When empty the engine relies on the standard GCP credential discovery.',
                    },
                ],
            },
        ], 'upstream');
    }
    if (comp.id === 'snk.motherduck') {
        // Mirror src.motherduck: compact form (database + token + schema +
        // tableName + mode) instead of the Snowflake-style warehouse fields.
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
                        description: 'Upsert deletes rows matching the conflict columns, then re-inserts (issue #19). Merge updates only the columns the source provides, leaving other target columns untouched (issue #39).',
                    },
                    {
                        key: 'conflictColumns',
                        label: 'Conflict columns (upsert key)',
                        kind: 'columns',
                        description: 'Key columns to match on for Upsert. Required when Write mode is Upsert.',
                    },
                ],
            },
        ], 'upstream');
    }
    if (comp.id === 'snk.quack') {
        // Mirror src.quack with an added write-mode picker. Reuses the
        // standard relational sink path so append / overwrite / truncate
        // all behave the same as snk.postgres / snk.motherduck.
        return base(comp, [
            {
                label: 'Connection',
                fields: [
                    { key: 'host', label: 'Host', kind: 'text', required: true, placeholder: 'duck.example.com' },
                    { key: 'port', label: 'Port', kind: 'integer', defaultValue: 9494, description: 'Default Quack port is 9494.' },
                    { key: 'token', label: 'Token', kind: 'text', placeholder: 'super_secret', description: 'Auth token; matches the value passed to quack_serve(token=...).' },
                ],
            },
            {
                label: 'Destination',
                fields: [
                    { key: 'schemaName', label: 'Schema', kind: 'text', defaultValue: 'main' },
                    { key: 'tableName', label: 'Table', kind: 'text', required: true, placeholder: 'orders' },
                    {
                        key: 'mode',
                        label: 'Write mode',
                        kind: 'select',
                        defaultValue: 'overwrite',
                        options: [
                            { label: 'Create or replace', value: 'overwrite' },
                            { label: 'Append (insert)', value: 'append' },
                            { label: 'Truncate + insert', value: 'truncate' },
                        ],
                    },
                ],
            },
        ], 'upstream');
    }
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
                    ...credentialFields('snowflake'),
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
                    // Default earliest: this is a batch ETL connector (capped by
                    // maxRecords), so a fresh run should read the available
                    // backlog, not start at the tip and see ~nothing. This also
                    // matches the engine's absent-offset default, so an untouched
                    // node's displayed default and its actual behavior agree.
                    label: 'Initial offset',
                    kind: 'select',
                    defaultValue: 'earliest',
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
                        { label: 'Bearer token', value: 'bearer' },
                        { label: 'API key (header)', value: 'apikey' },
                    ],
                },
                { key: 'authToken', label: 'Token / API key', kind: 'text', placeholder: '••••••••' },
                { key: 'authHeader', label: 'API key header', kind: 'text', placeholder: 'X-API-Key', description: 'Header name for API key auth (e.g. X-API-Key or X-Redmine-API-Key). Used only when Auth type is API key; leave blank to default to X-API-Key.' },
            ],
        },
        {
            label: 'Response',
            fields: [
                {
                    key: 'responsePath',
                    label: 'Records JSON pointer',
                    kind: 'text',
                    placeholder: '/data',
                    description: 'RFC 6901 JSON pointer to the array of row objects in the response. Leave empty if the response root IS the array.',
                },
                {
                    key: 'jsonPath',
                    label: 'Records JSONPath (legacy)',
                    kind: 'text',
                    placeholder: '$.data[*]',
                    description: 'JSONPath form for compatibility with older pipelines. Prefer the JSON pointer above.',
                },
            ],
        },
        {
            label: 'Pagination',
            fields: [
                {
                    key: 'paginationType',
                    label: 'Style',
                    kind: 'select',
                    defaultValue: 'none',
                    options: [
                        { label: 'None (single-shot fetch)', value: 'none' },
                        { label: 'Cursor (token in response body)', value: 'cursor' },
                        { label: 'Offset / limit', value: 'offset' },
                        { label: 'Page number', value: 'page' },
                        { label: 'RFC 5988 Link header (rel="next")', value: 'link' },
                    ],
                },
                {
                    key: 'cursorNextPath',
                    label: 'Cursor JSON pointer (cursor style)',
                    kind: 'text',
                    placeholder: '/meta/next_cursor',
                },
                {
                    key: 'cursorParam',
                    label: 'Cursor query parameter (cursor style)',
                    kind: 'text',
                    placeholder: 'cursor',
                },
                {
                    key: 'offsetParam',
                    label: 'Offset query parameter (offset style)',
                    kind: 'text',
                    placeholder: 'offset',
                },
                {
                    key: 'pageSize',
                    label: 'Page size (offset style)',
                    kind: 'integer',
                    defaultValue: 100,
                },
                {
                    key: 'totalCountPath',
                    label: 'Total-count JSON pointer (offset style)',
                    kind: 'text',
                    placeholder: '/total_count',
                    description: 'Optional. JSON pointer to a total-row count in the response (e.g. /total_count for Redmine). When set, offset paging stops once offset + page size reaches the total, instead of relying on a short page.',
                },
                {
                    key: 'pageParam',
                    label: 'Page query parameter (page style)',
                    kind: 'text',
                    placeholder: 'page',
                },
                {
                    key: 'startPage',
                    label: 'Start page (page style)',
                    kind: 'integer',
                    defaultValue: 1,
                },
                {
                    key: 'maxPages',
                    label: 'Max pages (safety cap)',
                    kind: 'integer',
                    defaultValue: 100,
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
                    { key: 'authHeader', label: 'API key header', kind: 'text', placeholder: 'X-API-Key', description: 'Header name for API key auth (e.g. X-API-Key or X-Redmine-API-Key). Used only when Auth type is API key; leave blank to default to X-API-Key.' },
                ],
            },
        ],
        'upstream',
    );
}

function synthNoSqlSource(comp: ComponentDef): ComponentManifest {
    if (comp.id === 'src.cassandra' || comp.id === 'src.scylla') {
        const vendor = comp.id === 'src.cassandra' ? 'Cassandra' : 'ScyllaDB';
        return base(comp, [
            {
                label: `${vendor} cluster`,
                fields: [
                    { key: 'contactPoints', label: 'Contact points', kind: 'text', required: true, placeholder: '127.0.0.1:9042,host2:9042' },
                    { key: 'user', label: 'User (optional)', kind: 'text' },
                    { key: 'password', label: 'Password (optional)', kind: 'text', placeholder: '••••••••' },
                    { key: 'keyspace', label: 'Keyspace', kind: 'text', placeholder: 'my_keyspace' },
                ],
            },
            {
                label: 'Query',
                fields: [
                    { key: 'tableName', label: 'Table (for SELECT *)', kind: 'text', placeholder: 'users' },
                    { key: 'query', label: 'Or custom CQL', kind: 'expression', rows: 4, placeholder: 'SELECT * FROM ks.tbl WHERE ...' },
                ],
            },
        ]);
    }
    if (comp.id === 'src.mongodb') {
        return base(comp, [
            {
                label: 'MongoDB connection',
                fields: [
                    { key: 'uri', label: 'Connection URI', kind: 'text', required: true, placeholder: 'mongodb://user:pass@host:27017' },
                    { key: 'database', label: 'Database', kind: 'text', required: true },
                    { key: 'collection', label: 'Collection', kind: 'text', required: true },
                ],
            },
            {
                label: 'Query',
                fields: [
                    {
                        key: 'filter',
                        label: 'Filter (JSON / extended JSON)',
                        kind: 'textarea',
                        rows: 4,
                        placeholder: '{"status": "active"}',
                        description: 'BSON document expressed as JSON. Empty = match all.',
                    },
                    {
                        key: 'projection',
                        label: 'Projection (JSON)',
                        kind: 'textarea',
                        rows: 2,
                        placeholder: '{"name": 1, "_id": 0}',
                    },
                    { key: 'limit', label: 'Limit (optional)', kind: 'integer' },
                ],
            },
        ]);
    }
    if (comp.id === 'src.elastic' || comp.id === 'src.opensearch') {
        const vendor = comp.id === 'src.elastic' ? 'Elasticsearch' : 'OpenSearch';
        return base(comp, [
            {
                label: `${vendor} cluster`,
                fields: [
                    { key: 'endpoint', label: 'Cluster endpoint', kind: 'text', required: true, placeholder: 'https://my-cluster.es.cloud.es.io' },
                    { key: 'index', label: 'Index (or pattern)', kind: 'text', required: true, placeholder: 'docs' },
                    { key: 'apiKey', label: 'API key', kind: 'text', placeholder: '••••••••' },
                ],
            },
            {
                label: 'Query',
                fields: [
                    {
                        key: 'query',
                        label: 'Query DSL (raw JSON)',
                        kind: 'textarea',
                        rows: 4,
                        placeholder: '{"match": {"status": "active"}}',
                        description: 'Body of the `query` field in the _search request. Empty = {"match_all": {}}.',
                    },
                    { key: 'size', label: 'Page size', kind: 'integer', defaultValue: 1000 },
                    { key: 'maxPages', label: 'Max pages (safety cap)', kind: 'integer', defaultValue: 100 },
                ],
            },
        ]);
    }
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
    if (comp.id === 'snk.cassandra' || comp.id === 'snk.scylla') {
        const vendor = comp.id === 'snk.cassandra' ? 'Cassandra' : 'ScyllaDB';
        return base(comp, [
            {
                label: `${vendor} cluster`,
                fields: [
                    { key: 'contactPoints', label: 'Contact points', kind: 'text', required: true, placeholder: '127.0.0.1:9042,host2:9042' },
                    { key: 'user', label: 'User (optional)', kind: 'text' },
                    { key: 'password', label: 'Password (optional)', kind: 'text', placeholder: '••••••••' },
                    { key: 'keyspace', label: 'Keyspace', kind: 'text', required: true },
                    { key: 'tableName', label: 'Table', kind: 'text', required: true },
                    { key: 'batchSize', label: 'Batch size (descriptive - CQL does single-row INSERTs)', kind: 'integer', defaultValue: 1000 },
                ],
            },
        ], 'upstream');
    }
    if (comp.id === 'snk.mongodb') {
        return base(comp, [
            {
                label: 'MongoDB connection',
                fields: [
                    { key: 'uri', label: 'Connection URI', kind: 'text', required: true, placeholder: 'mongodb://user:pass@host:27017' },
                    { key: 'database', label: 'Database', kind: 'text', required: true },
                    { key: 'collection', label: 'Collection', kind: 'text', required: true },
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
                            { label: 'Insert (insert_many)', value: 'insert' },
                            { label: 'Replace collection (drop + insert)', value: 'replace' },
                            { label: 'Upsert (replace_one on key)', value: 'upsert' },
                        ],
                    },
                    {
                        key: 'conflictColumns',
                        label: 'Upsert key fields',
                        kind: 'columns',
                        description: 'Upsert mode: document fields that form the match filter for replace_one(upsert=true). Required when Write mode is Upsert.',
                    },
                    {
                        key: 'deleteColumn',
                        label: 'Delete flag field (optional)',
                        kind: 'text',
                        placeholder: '_change_type',
                        description: 'Upsert only: documents whose value in this field equals the Delete value are delete_one\'d by key instead of upserted.',
                    },
                    {
                        key: 'deleteValue',
                        label: 'Delete flag value',
                        kind: 'text',
                        defaultValue: 'delete',
                        description: 'The value that marks a document for deletion (default "delete").',
                    },
                    {
                        key: 'batchSize',
                        label: 'Batch size',
                        kind: 'integer',
                        defaultValue: 1000,
                        description: 'Docs per insert_many call (insert / replace mode).',
                    },
                ],
            },
        ], 'upstream');
    }
    if (comp.id === 'snk.elastic' || comp.id === 'snk.opensearch') {
        const vendor = comp.id === 'snk.elastic' ? 'Elasticsearch' : 'OpenSearch';
        return base(
            comp,
            [
                {
                    label: `${vendor} cluster`,
                    fields: [
                        { key: 'endpoint', label: 'Cluster endpoint', kind: 'text', required: true, placeholder: 'https://my-cluster.es.cloud.es.io' },
                        { key: 'index', label: 'Index', kind: 'text', required: true, placeholder: 'docs' },
                        { key: 'apiKey', label: 'API key', kind: 'text', placeholder: '••••••••' },
                    ],
                },
                {
                    label: 'Body shape',
                    fields: [
                        {
                            key: 'shapeHint',
                            label: 'Row shape',
                            kind: 'text',
                            description: `Each upstream row is sent as a doc, preceded by a {"index":{"_index":"<index>"}} action line. Content-Type is application/x-ndjson.`,
                        },
                    ],
                },
            ],
            'upstream',
        );
    }
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
                    { key: 'port', label: 'Port', kind: 'integer', defaultValue: 22,
                      description: 'SFTP: 22. FTP / FTPS: usually 21.' },
                    { key: 'user', label: 'Username', kind: 'text' },
                    { key: 'password', label: 'Password', kind: 'text', placeholder: '••••••••' },
                ],
            },
            {
                label: 'SFTP key auth (optional)',
                fields: [
                    { key: 'privateKeyPath', label: 'Private key file', kind: 'file-path',
                      description: 'OpenSSH private key for SFTP key-based auth (instead of a password).' },
                    { key: 'keyPassphrase', label: 'Key passphrase', kind: 'text', placeholder: '••••••••' },
                    { key: 'hostFingerprint', label: 'Host fingerprint', kind: 'text',
                      placeholder: 'SHA256:...',
                      description: 'Optional SFTP host-key pin. If set, the connection is refused unless the server key matches this SHA256 fingerprint.' },
                ],
            },
            {
                label: 'Files',
                fields: [
                    { key: 'directory', label: 'Remote directory', kind: 'text', required: true,
                      defaultValue: '.', placeholder: '/incoming' },
                    { key: 'pattern', label: 'Filename pattern', kind: 'text', placeholder: '*.csv' },
                    { key: 'maxFiles', label: 'Max files', kind: 'integer', defaultValue: 100 },
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
                        key: 'format',
                        label: 'Date/time format (optional)',
                        kind: 'text',
                        placeholder: 'e.g. %d/%m/%Y',
                        description: 'strptime format for parsing a string into a date/timestamp (e.g. %d/%m/%Y or %Y.%m.%d %H:%M:%S). Leave blank for ISO auto-detect; only used for date/timestamp targets.',
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
                    { key: 'expression', label: 'Expression', kind: 'expression', rows: 3, required: true,
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
    if (id === 'xf.uuid') {
        return base(comp, [
            { label: 'UUID', fields: [
                { key: 'outputColumn', label: 'Output column', kind: 'text', defaultValue: 'row_id' },
            ] },
        ], 'upstream');
    }
    if (id === 'xf.compare') {
        return base(comp, [
            {
                label: 'Compare columns',
                fields: [
                    { key: 'leftColumn', label: 'Left column', kind: 'column', required: true },
                    {
                        key: 'op',
                        label: 'Operator',
                        kind: 'select',
                        defaultValue: 'eq',
                        options: [
                            { label: '= (equal)', value: 'eq' },
                            { label: '!= (not equal)', value: 'neq' },
                            { label: '< (less than)', value: 'lt' },
                            { label: '<= (less or equal)', value: 'le' },
                            { label: '> (greater)', value: 'gt' },
                            { label: '>= (greater or equal)', value: 'ge' },
                        ],
                    },
                    { key: 'rightColumn', label: 'Right column', kind: 'column', required: true },
                    { key: 'outputColumn', label: 'Output column', kind: 'text', placeholder: '<left>_<op>_<right>' },
                ],
            },
        ], 'upstream');
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
    if (id === 'xf.rank.filter') {
        return base(comp, [
            {
                label: 'Top N per group',
                fields: [
                    { key: 'partitionBy', label: 'Group by (optional)', kind: 'columns', description: 'Leave empty for top N across the whole input.' },
                    { key: 'orderBy', label: 'Order by column', kind: 'column', required: true },
                    {
                        key: 'desc',
                        label: 'Direction',
                        kind: 'select',
                        defaultValue: 'true',
                        options: [
                            { label: 'Descending (top N largest)', value: 'true' },
                            { label: 'Ascending (top N smallest)', value: 'false' },
                        ],
                    },
                    { key: 'n', label: 'N (rows to keep per group)', kind: 'integer', defaultValue: 10 },
                ],
            },
        ], 'upstream');
    }
    if (id === 'xf.fill_forward') {
        return base(comp, [
            {
                label: 'Forward fill',
                fields: [
                    { key: 'column', label: 'Column to fill', kind: 'column', required: true },
                    { key: 'orderBy', label: 'Order by column', kind: 'column', required: true, description: 'The window is ordered by this column (usually a timestamp).' },
                    { key: 'partitionBy', label: 'Group by (optional)', kind: 'columns', description: 'Fill independently within each group.' },
                ],
            },
        ], 'upstream');
    }
    if (id === 'xf.fill_backward') {
        return base(comp, [
            {
                label: 'Backward fill',
                fields: [
                    { key: 'column', label: 'Column to fill', kind: 'column', required: true },
                    { key: 'orderBy', label: 'Order by column', kind: 'column', required: true, description: 'The window is ordered by this column (usually a timestamp). Nulls take the next non-null value at a later position in the order.' },
                    { key: 'partitionBy', label: 'Group by (optional)', kind: 'columns', description: 'Fill independently within each group.' },
                ],
            },
        ], 'upstream');
    }
    if (id === 'xf.fill_constant') {
        return base(comp, [
            {
                label: 'Constant fill',
                fields: [
                    { key: 'column', label: 'Column to fill', kind: 'column', required: true },
                    {
                        key: 'value',
                        label: 'Fill value',
                        kind: 'text',
                        required: true,
                        placeholder: 'unknown',
                        description: 'Numbers (e.g. 0, -1.5) pass through unquoted; anything else is treated as a string. Booleans true / false also pass through.',
                    },
                ],
            },
        ], 'upstream');
    }
    if (id === 'xf.topn' || id === 'xf.sample' || id === 'xf.skip') {
        return base(comp, [
            {
                label: id === 'xf.sample' ? 'Sample' : 'Limit',
                fields: [
                    { key: 'count', label: id === 'xf.sample' ? 'Sample size' : 'Row count', kind: 'integer', defaultValue: 100 },
                ],
            },
        ], 'upstream');
    }
    return synthGeneric(comp, 'upstream');
}

function synthAggregateTransform(comp: ComponentDef): ComponentManifest {
    if (comp.id === 'xf.cumulative') {
        return base(comp, [
            {
                label: 'Cumulative',
                fields: [
                    { key: 'column', label: 'Column', kind: 'column', required: true },
                    {
                        key: 'function',
                        label: 'Function',
                        kind: 'select',
                        defaultValue: 'sum',
                        options: ['sum', 'avg', 'count', 'min', 'max'].map(f => ({ label: f.toUpperCase(), value: f })),
                    },
                    { key: 'orderBy', label: 'Order by', kind: 'column', required: true },
                    { key: 'partitionBy', label: 'Group by (optional)', kind: 'columns' },
                    { key: 'outputColumn', label: 'Output column', kind: 'text', placeholder: '<column>_running_<function>' },
                ],
            },
        ], 'upstream');
    }
    if (comp.id === 'xf.approx.quantile') {
        return base(comp, [
            {
                label: 'Approx quantile',
                fields: [
                    { key: 'column', label: 'Column', kind: 'column', required: true },
                    {
                        key: 'quantile',
                        label: 'Quantile (0 - 1)',
                        kind: 'number',
                        defaultValue: 0.5,
                        description: '0.5 = median, 0.95 = p95, 0.99 = p99',
                    },
                    { key: 'groupBy', label: 'Group by (optional)', kind: 'columns' },
                    { key: 'outputColumn', label: 'Output column', kind: 'text', placeholder: '<column>_q50' },
                ],
            },
        ], 'declared');
    }
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
    if (comp.id === 'xf.join.spatial') {
        return base(comp, [
            {
                label: 'Spatial join',
                fields: [
                    { key: 'leftGeomColumn', label: 'Left geometry column', kind: 'text', required: true, placeholder: 'orders.point' },
                    { key: 'rightGeomColumn', label: 'Right geometry column', kind: 'text', required: true, placeholder: 'zones.polygon' },
                    {
                        key: 'relation',
                        label: 'Spatial relation',
                        kind: 'select',
                        defaultValue: 'intersects',
                        options: [
                            { label: 'Intersects (any overlap)', value: 'intersects' },
                            { label: 'Contains (left contains right)', value: 'contains' },
                            { label: 'Within (left within right)', value: 'within' },
                            { label: 'Touches', value: 'touches' },
                            { label: 'Crosses', value: 'crosses' },
                            { label: 'Overlaps', value: 'overlaps' },
                            { label: 'Equals', value: 'equals' },
                        ],
                    },
                    {
                        key: 'joinType',
                        label: 'Join type',
                        kind: 'select',
                        defaultValue: 'inner',
                        options: [
                            { label: 'INNER', value: 'inner' },
                            { label: 'LEFT', value: 'left' },
                        ],
                    },
                ],
            },
        ], 'declared');
    }
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
                { key: 'ntileBuckets', label: 'Buckets (ntile)', kind: 'integer', defaultValue: 4 },
            ],
        },
        {
            label: 'Window',
            fields: [
                { key: 'partitionBy', label: 'Partition by', kind: 'columns' },
                { key: 'orderBy', label: 'Order by', kind: 'columns' },
            ],
        },
        { label: 'Output', fields: [{ key: 'outputName', label: 'Output column name', kind: 'text', required: true, placeholder: 'row_num' }] },
    ], 'declared');
}

function synthStringTransform(comp: ComponentDef): ComponentManifest {
    if (comp.id === 'xf.text.match') {
        return base(comp, [
            {
                label: 'Text match',
                fields: [
                    { key: 'column', label: 'Column', kind: 'column', required: true },
                    { key: 'needle', label: 'Search term', kind: 'text', required: true, placeholder: 'foo' },
                    {
                        key: 'mode',
                        label: 'Mode',
                        kind: 'select',
                        defaultValue: 'contains',
                        options: [
                            { label: 'Contains substring', value: 'contains' },
                            { label: 'Starts with prefix', value: 'starts_with' },
                            { label: 'Ends with suffix', value: 'ends_with' },
                        ],
                    },
                    { key: 'outputColumn', label: 'Output column', kind: 'text', placeholder: '<column>_<mode>' },
                ],
            },
        ], 'upstream');
    }
    if (comp.id === 'xf.text.reverse') {
        return base(comp, [{ label: 'Reverse', fields: [
            { key: 'column', label: 'Column', kind: 'column', required: true },
            { key: 'outputColumn', label: 'Output column', kind: 'text', placeholder: '<column>_reversed' },
        ] }], 'upstream');
    }
    if (comp.id === 'xf.text.replace') {
        return base(comp, [{ label: 'Literal replace', fields: [
            { key: 'column', label: 'Column', kind: 'column', required: true },
            { key: 'search', label: 'Search string', kind: 'text', required: true },
            { key: 'replacement', label: 'Replacement', kind: 'text' },
            { key: 'outputColumn', label: 'Output column', kind: 'text', placeholder: 'leave blank to overwrite' },
        ] }], 'upstream');
    }
    if (comp.id === 'xf.text.slug') {
        return base(comp, [{ label: 'URL slug', fields: [
            { key: 'column', label: 'Column', kind: 'column', required: true },
            { key: 'outputColumn', label: 'Output column', kind: 'text', placeholder: '<column>_slug' },
        ] }], 'upstream');
    }
    if (comp.id === 'xf.text.strip_html') {
        return base(comp, [{ label: 'Strip HTML', fields: [
            { key: 'column', label: 'Column', kind: 'column', required: true },
            { key: 'outputColumn', label: 'Output column', kind: 'text', placeholder: 'leave blank to overwrite' },
        ] }], 'upstream');
    }
    if (comp.id === 'xf.text.repeat') {
        return base(comp, [{ label: 'Repeat', fields: [
            { key: 'column', label: 'Column', kind: 'column', required: true },
            { key: 'count', label: 'Times', kind: 'integer', defaultValue: 2 },
            { key: 'outputColumn', label: 'Output column', kind: 'text', placeholder: '<column>_repeated' },
        ] }], 'upstream');
    }
    if (comp.id === 'xf.text.padding') {
        return base(comp, [
            {
                label: 'Pad string',
                fields: [
                    { key: 'column', label: 'Column', kind: 'column', required: true },
                    { key: 'length', label: 'Target length', kind: 'integer', required: true, defaultValue: 10 },
                    { key: 'fill', label: 'Fill character', kind: 'text', defaultValue: ' ', placeholder: '0' },
                    {
                        key: 'side',
                        label: 'Side',
                        kind: 'select',
                        defaultValue: 'left',
                        options: [
                            { label: 'Left (lpad - zero-pad numeric IDs)', value: 'left' },
                            { label: 'Right (rpad - fixed-width output)', value: 'right' },
                        ],
                    },
                    { key: 'outputColumn', label: 'Output column', kind: 'text', placeholder: 'leave blank to overwrite' },
                ],
            },
        ], 'upstream');
    }
    if (comp.id === 'xf.text.base64') {
        return base(comp, [
            {
                label: 'Base64',
                fields: [
                    { key: 'column', label: 'Column', kind: 'column', required: true },
                    {
                        key: 'mode',
                        label: 'Mode',
                        kind: 'select',
                        defaultValue: 'encode',
                        options: [
                            { label: 'Encode (text/bytes -> base64 text)', value: 'encode' },
                            { label: 'Decode (base64 text -> bytes-as-text)', value: 'decode' },
                        ],
                    },
                    { key: 'outputColumn', label: 'Output column', kind: 'text', placeholder: '<column>_<mode>' },
                ],
            },
        ], 'upstream');
    }
    if (comp.id === 'xf.text.similarity') {
        return base(comp, [
            {
                label: 'Text similarity',
                fields: [
                    { key: 'leftColumn', label: 'Left column', kind: 'column', required: true },
                    { key: 'rightColumn', label: 'Right column', kind: 'column', required: true },
                    {
                        key: 'algorithm',
                        label: 'Algorithm',
                        kind: 'select',
                        defaultValue: 'levenshtein',
                        options: [
                            { label: 'Levenshtein (edit distance, integer)', value: 'levenshtein' },
                            { label: 'Damerau-Levenshtein (adds transpositions)', value: 'damerau_levenshtein' },
                            { label: 'Jaccard (trigram set similarity, 0-1)', value: 'jaccard' },
                            { label: 'Jaro-Winkler (similarity, 0-1, prefix-weighted)', value: 'jaro_winkler' },
                        ],
                    },
                    { key: 'outputColumn', label: 'Output column', kind: 'text', placeholder: '<left>_<right>_score' },
                ],
            },
        ], 'upstream');
    }
    if (comp.id === 'xf.regex.match') {
        return base(comp, [
            {
                label: 'Regex match',
                fields: [
                    { key: 'column', label: 'Column', kind: 'column', required: true },
                    { key: 'pattern', label: 'Pattern', kind: 'text', required: true, placeholder: '^[A-Z]+$' },
                    { key: 'outputColumn', label: 'Output column', kind: 'text', placeholder: '<column>_matches' },
                ],
            },
        ], 'upstream');
    }
    if (comp.id === 'xf.url.parse') {
        return base(comp, [
            {
                label: 'URL parse',
                fields: [
                    { key: 'column', label: 'URL column', kind: 'column', required: true },
                    {
                        key: 'kind',
                        label: 'Extract',
                        kind: 'select',
                        defaultValue: 'host',
                        options: [
                            { label: 'Scheme (http, https, ...)', value: 'scheme' },
                            { label: 'Host', value: 'host' },
                            { label: 'Port', value: 'port' },
                            { label: 'Path', value: 'path' },
                            { label: 'Query string', value: 'query' },
                            { label: 'Fragment (#...)', value: 'fragment' },
                        ],
                    },
                    { key: 'outputColumn', label: 'Output column', kind: 'text', placeholder: '<column>_<kind>' },
                ],
            },
        ], 'upstream');
    }
    if (comp.id === 'xf.regex.extract') {
        return base(comp, [
            {
                label: 'Regex extract',
                fields: [
                    { key: 'column', label: 'Column', kind: 'column', required: true },
                    { key: 'pattern', label: 'Pattern', kind: 'text', required: true, placeholder: '([0-9]+)' },
                    {
                        key: 'groupIndex',
                        label: 'Group',
                        kind: 'number',
                        defaultValue: 0,
                        description: '0 = whole match, 1 = first capture group, 2 = second, ...',
                    },
                    { key: 'outputColumn', label: 'Output column', kind: 'text', placeholder: 'leave blank to overwrite' },
                ],
            },
        ], 'upstream');
    }
    if (comp.id === 'xf.ip.parse') {
        return base(comp, [
            {
                label: 'IP Parse',
                fields: [
                    { key: 'column', label: 'IP / CIDR column', kind: 'column', required: true },
                    {
                        key: 'kind',
                        label: 'Extract',
                        kind: 'select',
                        defaultValue: 'host',
                        options: [
                            { label: 'Host (address without mask)', value: 'host' },
                            { label: 'Family (4 or 6)', value: 'family' },
                            { label: 'Broadcast address', value: 'broadcast' },
                            { label: 'Netmask', value: 'netmask' },
                            { label: 'Hostmask', value: 'hostmask' },
                            { label: 'Mask length (bits)', value: 'masklen' },
                            { label: 'Network (address & netmask)', value: 'network' },
                        ],
                    },
                    { key: 'outputColumn', label: 'Output column', kind: 'text', placeholder: '<column>_<kind>' },
                ],
            },
        ], 'upstream');
    }
    if (comp.id === 'xf.hash') {
        return base(comp, [
            {
                label: 'Hash',
                fields: [
                    { key: 'column', label: 'Column to hash', kind: 'column', required: true },
                    {
                        key: 'algorithm',
                        label: 'Algorithm',
                        kind: 'select',
                        defaultValue: 'md5',
                        options: [
                            { label: 'MD5 (hex string)', value: 'md5' },
                            { label: 'SHA-1 (hex string)', value: 'sha1' },
                            { label: 'SHA-256 (hex string)', value: 'sha256' },
                            { label: 'DuckDB hash() (int64, fast non-cryptographic)', value: 'hash' },
                        ],
                    },
                    { key: 'outputColumn', label: 'Output column', kind: 'text', placeholder: '<column>_hash' },
                ],
            },
        ], 'upstream');
    }
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

const TIME_UNITS = ['year', 'quarter', 'month', 'week', 'day', 'hour', 'minute', 'second', 'dayofweek', 'isodow', 'dayofyear', 'epoch'];
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
    if (id === 'xf.dt.now') {
        return base(comp, [{ label: 'Current timestamp', fields: [
            { key: 'outputColumn', label: 'Output column', kind: 'text', defaultValue: 'loaded_at' },
        ] }], 'upstream');
    }
    if (id === 'xf.dt.epoch') {
        return base(comp, [{ label: 'Epoch convert', fields: [
            col,
            {
                key: 'mode',
                label: 'Direction',
                kind: 'select',
                defaultValue: 'to',
                options: [
                    { label: 'Timestamp -> epoch seconds', value: 'to' },
                    { label: 'Epoch seconds -> timestamp', value: 'from' },
                ],
            },
            { key: 'outputColumn', label: 'Output column', kind: 'text', placeholder: '<column>_epoch / <column>_timestamp' },
        ] }], 'upstream');
    }
    if (id === 'xf.dt.bin') {
        return base(comp, [{ label: 'Time bin', fields: [
            col,
            { key: 'count', label: 'Bucket size', kind: 'integer', defaultValue: 5 },
            {
                key: 'unit',
                label: 'Unit',
                kind: 'select',
                defaultValue: 'minute',
                options: ['second', 'minute', 'hour', 'day'].map(u => ({ label: u, value: u })),
            },
            outColField(),
        ] }], 'upstream');
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
    if (comp.id === 'xf.num.sign') {
        return base(comp, [
            {
                label: 'Sign',
                fields: [
                    { key: 'column', label: 'Column', kind: 'column', required: true },
                    { key: 'outputColumn', label: 'Output column', kind: 'text', placeholder: '<column>_sign' },
                ],
            },
        ], 'upstream');
    }
    if (comp.id === 'xf.num.clamp') {
        return base(comp, [
            {
                label: 'Clamp',
                fields: [
                    { key: 'column', label: 'Column', kind: 'column', required: true },
                    { key: 'low', label: 'Low bound', kind: 'number', required: true, defaultValue: 0 },
                    { key: 'high', label: 'High bound', kind: 'number', required: true, defaultValue: 100 },
                ],
            },
        ], 'upstream');
    }
    if (comp.id === 'xf.num.zscore') {
        return base(comp, [
            {
                label: 'Z-Score',
                fields: [
                    { key: 'column', label: 'Column', kind: 'column', required: true },
                    { key: 'outputColumn', label: 'Output column', kind: 'text', placeholder: '<column>_zscore' },
                ],
            },
        ], 'upstream');
    }
    if (comp.id === 'xf.num.bucketize') {
        return base(comp, [
            {
                label: 'Bucketize',
                fields: [
                    { key: 'column', label: 'Column', kind: 'column', required: true },
                    { key: 'low', label: 'Low bound', kind: 'number', required: true, defaultValue: 0 },
                    { key: 'high', label: 'High bound', kind: 'number', required: true, defaultValue: 100 },
                    { key: 'buckets', label: 'Number of buckets', kind: 'integer', defaultValue: 10 },
                    { key: 'outputColumn', label: 'Output column', kind: 'text', placeholder: '<column>_bucket' },
                ],
            },
        ], 'upstream');
    }
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
    if (id === 'xf.json.array_agg') {
        return base(comp, [
            {
                label: 'Array aggregate',
                fields: [
                    { key: 'column', label: 'Column to collect', kind: 'column', required: true },
                    {
                        key: 'groupBy',
                        label: 'Group by (optional)',
                        kind: 'columns',
                        description: 'Leave empty to collapse the entire input into one array.',
                    },
                    { key: 'outputColumn', label: 'Output column', kind: 'text', placeholder: '<column>_array' },
                ],
            },
        ], 'declared');
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
    if (id === 'xf.arr.length') {
        return base(comp, [{ label: 'Array length', fields: [
            col,
            { key: 'outputColumn', label: 'Output column', kind: 'text', placeholder: '<column>_length' },
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
    if (id === 'xf.zip') {
        return base(comp, [{ label: 'Zip arrays to table', fields: [
            { key: 'headingsColumn', label: 'Headings column', kind: 'column', required: true,
              description: 'A list column of column names, e.g. headings = ["col1","col2","col3"].' },
            { key: 'valuesColumn', label: 'Rows column', kind: 'column', required: true,
              description: 'A list-of-lists column; each inner array becomes one output row, its values aligned to the headings by position.' },
        ] }], 'declared');
    }
    // distinct
    return base(comp, [{ label: 'Array distinct', fields: [col, outColField()] }], 'upstream');
}

function synthCdcTransform(comp: ComponentDef): ComponentManifest {
    const id = comp.id;
    if (id === 'xf.diffsummary') {
        return base(comp, [
            {
                label: 'Diff summary',
                fields: [
                    {
                        key: 'changeColumn',
                        label: 'Change-type column',
                        kind: 'text',
                        defaultValue: 'change_type',
                        description: 'The column holding insert / delete / update_postimage values (the DuckLake Data Diff feed uses change_type). Emits added / removed / updated / total_changes + a ready summary text - feed that row into LLM Transform for an AI narrative, or a validator to assert counts.',
                    },
                ],
            },
        ], 'upstream');
    }
    if (id === 'xf.incremental') {
        return base(comp, [
            {
                label: 'Incremental load',
                fields: [
                    {
                        key: 'column',
                        label: 'Watermark column',
                        kind: 'column',
                        required: true,
                        description: 'Monotonic column - a timestamp (updated_at) or an increasing id. Only rows past the last successful run pass through.',
                    },
                    {
                        key: 'initialValue',
                        label: 'Initial value (first run)',
                        kind: 'text',
                        placeholder: 'e.g. 2024-01-01 or 0',
                        description: 'Watermark to start from on the very first run, before any state is saved. Leave empty to load everything once.',
                    },
                ],
            },
        ], 'upstream');
    }
    if (id === 'xf.row_hash') {
        return base(comp, [
            {
                label: 'Row hash',
                fields: [
                    {
                        key: 'columns',
                        label: 'Columns to hash',
                        kind: 'columns',
                        required: true,
                        description: 'Listed in this order. concat_ws("||", col1, col2, ...) then hashed.',
                    },
                    {
                        key: 'algorithm',
                        label: 'Algorithm',
                        kind: 'select',
                        defaultValue: 'md5',
                        options: [
                            { label: 'md5  (16-byte hex, fastest)', value: 'md5' },
                            { label: 'sha1  (20-byte hex)', value: 'sha1' },
                            { label: 'sha256  (32-byte hex, collision-safe)', value: 'sha256' },
                        ],
                    },
                    {
                        key: 'outputColumn',
                        label: 'Output column',
                        kind: 'text',
                        defaultValue: '_row_hash',
                    },
                ],
            },
        ], 'upstream');
    }
    if (id === 'xf.audit') {
        return base(comp, [
            {
                label: 'Audit columns',
                fields: [
                    { key: 'loadedAt', label: 'Add _loaded_at (current_timestamp)', kind: 'bool', defaultValue: true },
                    { key: 'loadedDate', label: 'Add _loaded_date (current_date)', kind: 'bool', defaultValue: false },
                    {
                        key: 'source',
                        label: 'Source label (_source)',
                        kind: 'text',
                        placeholder: 'orders_etl',
                        description: 'String literal stamped on every row. Use {{ context.var }} to pull a per-run value.',
                    },
                    {
                        key: 'batchId',
                        label: 'Batch ID (_batch_id)',
                        kind: 'text',
                        placeholder: '{{ context.run_id }}',
                    },
                ],
            },
        ], 'upstream');
    }
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
        // Both run a CHILD pipeline (pipelineRef): iterate runs it `count`
        // times (${ITER_INDEX}); foreach runs it once per upstream row
        // (${ITER_ITEM_<COLUMN>} + ${ITER_INDEX}). Without pipelineRef the run
        // fails with "pipelineRef required" - the old Loop fields (variable /
        // from / to / collection) were never read by the engine (issue #26).
        const isIterate = id === 'ctl.iterate';
        const fields: Field[] = [
            {
                key: 'pipelineRef',
                label: 'Pipeline to run',
                kind: 'pipeline-ref',
                required: true,
                description: isIterate
                    ? 'Child pipeline, run once per iteration. Use ${ITER_INDEX} (0-based) inside it.'
                    : 'Child pipeline, run once per upstream row. Use ${ITER_ITEM_<COLUMN>} (uppercased) and ${ITER_INDEX} inside it.',
            },
        ];
        if (isIterate) {
            fields.push({
                key: 'count',
                label: 'Iterations',
                kind: 'integer',
                defaultValue: 1,
                required: true,
                description: 'How many times to run the child pipeline.',
            });
        } else {
            fields.push({
                key: 'concurrency',
                label: 'Concurrency',
                kind: 'integer',
                defaultValue: 1,
                description: 'How many rows to process at the same time. 1 runs sequentially; higher overlaps slow per-row work (e.g. a cloud sink that re-connects each run). Each row still runs in isolation.',
            });
        }
        return base(comp, [{ label: isIterate ? 'Iterate' : 'For each row', fields }], 'upstream');
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
    if (comp.id === 'ctl.runpipeline' || comp.id === 'ctl.trigger' || comp.id === 'ctl.runjob') {
        const isJob = comp.id === 'ctl.runjob';
        return base(comp, [
            {
                label: isJob ? 'Child job' : 'Pipeline',
                fields: [
                    { key: 'pipelineRef', label: isJob ? 'Child job / pipeline' : 'Pipeline', kind: 'pipeline-ref', required: true, description: 'Pick a pipeline from this workspace.' },
                    { key: 'waitForCompletion', label: 'Wait for completion', kind: 'bool', defaultValue: true },
                    {
                        key: isJob ? 'contextVariables' : 'parameters',
                        label: isJob ? 'Context variables' : 'Parameters',
                        kind: 'key-value',
                    },
                ],
            },
        ], 'upstream');
    }
    if (comp.id === 'ctl.parallelize') {
        return base(comp, [
            {
                label: 'Parallelize',
                fields: [
                    {
                        key: 'maxConcurrency',
                        label: 'Max concurrent branches',
                        kind: 'integer',
                        defaultValue: 0,
                        placeholder: '0 = auto',
                        description:
                            '0 = auto: runs one branch per CPU core (capped to the branch count). Set a number to cap concurrency explicitly.',
                    },
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

function synthLoggingControl(comp: ComponentDef): ComponentManifest {
    const id = comp.id;
    if (id === 'ctl.log' || id === 'ctl.warn') {
        const isWarn = id === 'ctl.warn';
        return base(comp, [
            {
                label: isWarn ? 'Warn' : 'Log message',
                fields: [
                    {
                        key: 'message',
                        label: 'Message',
                        kind: 'text',
                        required: true,
                        placeholder: isWarn ? 'Unexpected {rows} rows' : 'Processed {rows} rows',
                        description: 'Use {rows} for the upstream row count. Written to the workspace run log (logs/duckle.jsonl).',
                    },
                ],
            },
        ], 'upstream');
    }
    if (id === 'ctl.die') {
        return base(comp, [
            {
                label: 'Die / Fail',
                fields: [
                    {
                        key: 'message',
                        label: 'Error message',
                        kind: 'text',
                        required: true,
                        placeholder: 'Rejected rows present - failing the run',
                        description: 'Use {rows} for the upstream row count.',
                    },
                    {
                        key: 'condition',
                        label: 'Fire when',
                        kind: 'select',
                        defaultValue: 'always',
                        options: [
                            { label: 'Always', value: 'always' },
                            { label: 'Input has rows', value: 'has-rows' },
                            { label: 'Input is empty', value: 'no-rows' },
                        ],
                        description: 'Always stop, or only when the input has / has no rows (guard a reject branch or missing data).',
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
    if (id === 'qa.mask') {
        return base(comp, [
            {
                label: 'Mask / Anonymize',
                fields: [
                    { key: 'column', label: 'Column', kind: 'column', required: true },
                    {
                        key: 'mode',
                        label: 'Mode',
                        kind: 'select',
                        defaultValue: 'hash',
                        options: [
                            { label: 'Hash (deterministic pseudonym)', value: 'hash' },
                            { label: 'Partial (show last N)', value: 'partial' },
                            { label: 'Null out', value: 'null' },
                            { label: 'Constant', value: 'constant' },
                        ],
                    },
                    { key: 'salt', label: 'Salt (hash mode)', kind: 'text', description: 'Optional secret mixed in before hashing; the same value maps to the same token, and a shared salt keeps masked datasets joinable.' },
                    { key: 'showLast', label: 'Show last N (partial mode)', kind: 'integer', defaultValue: 4 },
                    { key: 'value', label: 'Replacement (constant mode)', kind: 'text', placeholder: 'REDACTED' },
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
                    filters: [{ name: 'WebAssembly', extensions: ['wasm'] }] },
                  { key: 'reuseInstance', label: 'Reuse module instance across rows', kind: 'bool' as const,
                    defaultValue: false,
                    placeholder: 'Faster, but module memory/state persists between rows (default: fresh instance per row)' }] : []),
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
    if (comp.id === 'snk.pinecone') {
        return base(comp, [
            {
                label: 'Pinecone index',
                fields: [
                    {
                        key: 'indexHost',
                        label: 'Index host',
                        kind: 'text',
                        required: true,
                        placeholder: 'idx-abc123.svc.us-east1-gcp.pinecone.io',
                        description: 'The host part of your index URL. Strip the leading https://.',
                    },
                    { key: 'apiKey', label: 'API key', kind: 'text', required: true, placeholder: '••••••••' },
                ],
            },
            {
                label: 'Body shape',
                fields: [
                    {
                        key: 'shapeHint',
                        label: 'Row shape',
                        kind: 'text',
                        description: 'Each upstream row should already have {id, values, metadata}. Use a Project / Add Column upstream to rename your embedding column to "values" and any extras into a "metadata" struct.',
                    },
                ],
            },
        ], 'upstream');
    }
    if (comp.id === 'snk.qdrant') {
        return base(comp, [
            {
                label: 'Qdrant cluster',
                fields: [
                    {
                        key: 'clusterUrl',
                        label: 'Cluster URL',
                        kind: 'text',
                        required: true,
                        placeholder: 'https://xyz-east1.aws.cloud.qdrant.io:6333',
                    },
                    { key: 'collection', label: 'Collection', kind: 'text', required: true, placeholder: 'documents' },
                    { key: 'apiKey', label: 'API key', kind: 'text', placeholder: '••••••••' },
                ],
            },
            {
                label: 'Body shape',
                fields: [
                    {
                        key: 'shapeHint',
                        label: 'Row shape',
                        kind: 'text',
                        description: 'Each upstream row should already have {id, vector, payload}. Use Project / Add Column upstream to reshape if needed.',
                    },
                ],
            },
        ], 'upstream');
    }
    if (comp.id === 'snk.weaviate') {
        return base(comp, [
            {
                label: 'Weaviate cluster',
                fields: [
                    {
                        key: 'endpoint',
                        label: 'Cluster endpoint',
                        kind: 'text',
                        required: true,
                        placeholder: 'https://my-cluster.weaviate.network',
                    },
                    { key: 'apiKey', label: 'API key', kind: 'text', placeholder: '••••••••' },
                ],
            },
            {
                label: 'Body shape',
                fields: [
                    {
                        key: 'shapeHint',
                        label: 'Row shape',
                        kind: 'text',
                        description: 'Each upstream row should already have {class, properties, vector}. The engine wraps the batch in {objects: [...]}.',
                    },
                ],
            },
        ], 'upstream');
    }
    if (comp.id === 'snk.milvus') {
        return base(comp, [
            {
                label: 'Milvus cluster',
                fields: [
                    { key: 'endpoint', label: 'Cluster endpoint', kind: 'text', required: true, placeholder: 'https://in03-...api.cloud.zilliz.com' },
                    { key: 'collection', label: 'Collection', kind: 'text', required: true, placeholder: 'documents' },
                    { key: 'apiKey', label: 'API key (Bearer)', kind: 'text', placeholder: '••••••••' },
                ],
            },
            {
                label: 'Body shape',
                fields: [
                    {
                        key: 'shapeHint',
                        label: 'Row shape',
                        kind: 'text',
                        description: 'Each upstream row should have {id, vector, ...}. The engine wraps as {collectionName, data: [...]}.',
                    },
                ],
            },
        ], 'upstream');
    }
    if (comp.id === 'snk.pgvector') {
        return base(comp, [
            { label: 'Connection', fields: dbConnectionFields(comp.id) },
            { label: 'Destination', fields: [
                { key: 'schemaName', label: 'Schema', kind: 'text', defaultValue: 'public' },
                { key: 'tableName', label: 'Table', kind: 'text', required: true, placeholder: 'embeddings' },
                {
                    key: 'mode',
                    label: 'Write mode',
                    kind: 'select',
                    defaultValue: 'overwrite',
                    options: [
                        { label: 'Create or replace', value: 'overwrite' },
                        { label: 'Append (insert)', value: 'append' },
                        { label: 'Upsert on conflict', value: 'upsert' },
                        { label: 'Truncate + insert', value: 'truncate' },
                    ],
                },
                {
                    key: 'conflictColumns',
                    label: 'Conflict columns (for upsert)',
                    kind: 'columns',
                    description: 'Required when mode is upsert.',
                },
            ]},
        ], 'upstream');
    }
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
    if (comp.id === 'src.pgvector') {
        // pgvector tables live inside a regular Postgres server, so the
        // same connection + table form as src.postgres applies. Vector
        // columns come through DuckDB's postgres extension as FLOAT[N].
        return base(comp, [
            { label: 'Connection', fields: dbConnectionFields(comp.id) },
            { label: 'Table', fields: [
                { key: 'schemaName', label: 'Schema', kind: 'text', defaultValue: 'public' },
                { key: 'tableName', label: 'Table', kind: 'text', required: true, placeholder: 'embeddings' },
            ]},
        ]);
    }
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
    if (id === 'xf.ai.text_search') {
        return base(comp, [
            {
                label: 'Full-text search',
                fields: [
                    {
                        key: 'idColumn',
                        label: 'ID column',
                        kind: 'column',
                        required: true,
                        description: 'Unique row identifier; required by the BM25 index.',
                    },
                    {
                        key: 'textColumns',
                        label: 'Text columns to index',
                        kind: 'columns',
                        required: true,
                    },
                    {
                        key: 'query',
                        label: 'Search query',
                        kind: 'text',
                        required: true,
                        placeholder: 'duckdb analytics',
                    },
                    {
                        key: 'topK',
                        label: 'Top K',
                        kind: 'integer',
                        description: 'Optional: keep only the K highest-scoring rows.',
                    },
                    {
                        key: 'outputColumn',
                        label: 'Score column',
                        kind: 'text',
                        defaultValue: 'score',
                    },
                ],
            },
        ], 'upstream');
    }
    if (id === 'xf.ai.vector_search') {
        return base(comp, [
            {
                label: 'Vector similarity search',
                fields: [
                    {
                        key: 'vectorColumn',
                        label: 'Vector column',
                        kind: 'column',
                        required: true,
                        description: 'Column holding the embeddings (array of floats).',
                    },
                    {
                        key: 'targetVector',
                        label: 'Query vector',
                        kind: 'expression',
                        rows: 3,
                        required: true,
                        placeholder: '[0.1, 0.2, 0.3, ...]',
                        description: 'JSON array of floats, length must equal Dimension.',
                    },
                    {
                        key: 'dimension',
                        label: 'Dimension',
                        kind: 'integer',
                        required: true,
                        defaultValue: 384,
                    },
                    {
                        key: 'distanceMetric',
                        label: 'Distance metric',
                        kind: 'select',
                        defaultValue: 'cosine',
                        options: [
                            { label: 'Cosine similarity (higher = closer)', value: 'cosine' },
                            { label: 'L2 distance (lower = closer)', value: 'l2' },
                            { label: 'Inner product (higher = closer)', value: 'inner_product' },
                        ],
                    },
                    {
                        key: 'topK',
                        label: 'Top K',
                        kind: 'integer',
                        description: 'Leave empty to score every row; set K to keep only the K closest.',
                    },
                    {
                        key: 'outputColumn',
                        label: 'Score column',
                        kind: 'text',
                        defaultValue: 'similarity_score',
                    },
                ],
            },
        ], 'upstream');
    }
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

function synthGeoTransform(comp: ComponentDef): ComponentManifest {
    if (comp.id === 'xf.geo.distance') {
        return base(comp, [
            {
                label: 'Spatial distance',
                fields: [
                    { key: 'geomColumn', label: 'Geometry column', kind: 'column', required: true },
                    {
                        key: 'targetWkt',
                        label: 'Target geometry (WKT)',
                        kind: 'text',
                        required: true,
                        placeholder: 'POINT(0 0)',
                        description: 'Well-Known Text. Distance units come from the input SRS (degrees for WGS84, metres for projected).',
                    },
                    { key: 'outputColumn', label: 'Output column', kind: 'text', defaultValue: 'distance' },
                ],
            },
        ], 'upstream');
    }
    if (comp.id === 'xf.geo.buffer') {
        return base(comp, [
            {
                label: 'Spatial buffer',
                fields: [
                    { key: 'geomColumn', label: 'Geometry column', kind: 'column', required: true },
                    { key: 'distance', label: 'Buffer distance', kind: 'number', required: true, defaultValue: 1.0 },
                    { key: 'outputColumn', label: 'Output column', kind: 'text', defaultValue: 'buffer' },
                ],
            },
        ], 'upstream');
    }
    if (comp.id === 'xf.geo.intersects') {
        return base(comp, [
            {
                label: 'Spatial intersects',
                fields: [
                    { key: 'geomColumn', label: 'Geometry column', kind: 'column', required: true },
                    {
                        key: 'targetWkt',
                        label: 'Target geometry (WKT)',
                        kind: 'text',
                        required: true,
                        placeholder: 'POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))',
                        description: 'Each row gets a boolean: does its geometry overlap this target? Pair with Filter Rows to keep only the matches.',
                    },
                    { key: 'outputColumn', label: 'Output column', kind: 'text', defaultValue: 'intersects' },
                ],
            },
        ], 'upstream');
    }
    return base(comp, [], 'upstream');
}

function synthDebugTransform(comp: ComponentDef): ComponentManifest {
    if (comp.id === 'xf.assert') {
        return base(comp, [
            {
                label: 'Assertion',
                fields: [
                    {
                        key: 'predicate',
                        label: 'SQL predicate (must be true on every row)',
                        kind: 'expression',
                        required: true,
                        rows: 2,
                        placeholder: 'amount >= 0',
                        description: 'Plain SQL boolean expression evaluated per row. If any row returns false, the pipeline raises with the message below.',
                    },
                    {
                        key: 'message',
                        label: 'Error message (optional)',
                        kind: 'text',
                        placeholder: 'amount must be non-negative',
                    },
                ],
            },
        ], 'upstream');
    }
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

// xf.dbt lives in the Custom Code palette section (group code.dbt) but needs
// its own settings, so it is dispatched by id at the top of synthesizeManifest
// rather than via the group table.
function synthDbt(comp: ComponentDef): ComponentManifest {
    return base(comp, [
        {
            label: 'Inline model',
            fields: [
                {
                    key: 'model',
                    label: 'Model SQL',
                    kind: 'textarea',
                    rows: 8,
                    placeholder:
                        "select country, sum(amount) as revenue\nfrom {{ var('duckle_input') }}\ngroup by country",
                    description: "Write one dbt model right here - no external project needed. Reference the upstream node with {{ var('duckle_input') }}. The engine scaffolds an ephemeral dbt project and runs it. Leave empty to use an existing project below instead.",
                },
                {
                    key: 'modelName',
                    label: 'Model name',
                    kind: 'text',
                    defaultValue: 'duckle_model',
                    description: 'Name of the inline model and its output table.',
                },
            ],
        },
        {
            label: 'Existing project (alternative to inline)',
            fields: [
                {
                    key: 'projectDir',
                    label: 'Project directory',
                    kind: 'text',
                    placeholder: 'C:\\work\\my_dbt_project',
                    description: 'Folder containing dbt_project.yml. Use this to run an existing dbt project instead of an inline model. The engine generates profiles.yml for the dbt-duckdb adapter pointed at this run.',
                },
                {
                    key: 'command',
                    label: 'dbt command',
                    kind: 'text',
                    defaultValue: 'run',
                    placeholder: 'run --select staging+',
                    description: 'dbt subcommand and flags, split on spaces. Examples: run, build (run + test), test, "run --select my_model".',
                },
                {
                    key: 'outputModel',
                    label: 'Output model (optional)',
                    kind: 'text',
                    placeholder: 'fct_daily_revenue',
                    description: 'Read this model back as the node output. Leave empty (project mode) to emit a per-model run summary. In inline mode this defaults to the model name.',
                },
            ],
        },
        {
            label: 'Advanced',
            fields: [
                {
                    key: 'schema',
                    label: 'Schema',
                    kind: 'text',
                    defaultValue: 'main',
                    description: 'Schema dbt materializes into (the generated profile target schema).',
                },
                {
                    key: 'database',
                    label: 'Target database file (optional)',
                    kind: 'text',
                    placeholder: 'leave empty to use the run database',
                    description: 'A specific DuckDB file to build into. Default: the pipeline run database, so dbt composes with the rest of the canvas.',
                },
                {
                    key: 'dbtBin',
                    label: 'dbt executable (optional)',
                    kind: 'text',
                    placeholder: 'dbt',
                    description: 'Path to dbt if it is not on PATH / not the bundled one. Needs the DuckDB adapter (dbt-duckdb).',
                },
                {
                    key: 'timeoutMs',
                    label: 'Timeout (ms, optional)',
                    kind: 'number',
                    placeholder: 'e.g. 600000',
                    description: 'Kill the dbt process after this many milliseconds. Empty = no timeout.',
                },
            ],
        },
    ]);
}

// Main entry ------------------------------------------------------------

export function synthesizeManifest(componentId: string): ComponentManifest | undefined {
    const entry = findPaletteEntry(componentId);
    if (!entry) return undefined;
    const { groupId, comp } = entry;

    // Id-specific dispatch for components whose palette group has no dedicated
    // synth path (e.g. xf.dbt sits in the Custom Code section).
    if (componentId === 'xf.dbt') return synthDbt(comp);

    // Sources
    if (groupId === 'src.files') return synthFileSource(comp);
    if (groupId === 'src.lakehouse') return synthLakehouseSource(comp);
    if (groupId === 'snk.lakehouse') return synthLakehouseSink(comp);
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
    if (groupId === 'xf.geo') return synthGeoTransform(comp);
    if (groupId === 'xf.debug') return synthDebugTransform(comp);

    // Control
    if (groupId === 'ctl.routing') return synthRoutingControl(comp);
    if (groupId === 'ctl.timing') return synthTimingControl(comp);
    if (groupId === 'ctl.pipeline') return synthPipelineControl(comp);
    if (groupId === 'ctl.errors') return synthErrorControl(comp);
    if (groupId === 'ctl.logging') return synthLoggingControl(comp);

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
