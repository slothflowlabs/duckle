export type Availability = 'available' | 'planned' | 'preview';

export type NodeKind = 'source' | 'transform' | 'sink' | 'control' | 'quality' | 'custom';

export type ComponentDef = {
    id: string;
    label: string;
    kind: NodeKind;
    availability: Availability;
    summary?: string;
};

export type Group = {
    id: string;
    label: string;
    components: ComponentDef[];
};

export type Category = {
    id: string;
    label: string;
    icon: string;
    accent: string;
    groups: Group[];
};

const src = (id: string, label: string, availability: Availability, summary?: string): ComponentDef => ({
    id: 'src.' + id,
    label,
    kind: 'source',
    availability,
    summary,
});

const snk = (id: string, label: string, availability: Availability, summary?: string): ComponentDef => ({
    id: 'snk.' + id,
    label,
    kind: 'sink',
    availability,
    summary,
});

const xf = (id: string, label: string, availability: Availability, summary?: string): ComponentDef => ({
    id: 'xf.' + id,
    label,
    kind: 'transform',
    availability,
    summary,
});

const ctl = (id: string, label: string, availability: Availability, summary?: string): ComponentDef => ({
    id: 'ctl.' + id,
    label,
    kind: 'control',
    availability,
    summary,
});

const qa = (id: string, label: string, availability: Availability, summary?: string): ComponentDef => ({
    id: 'qa.' + id,
    label,
    kind: 'quality',
    availability,
    summary,
});

const code = (id: string, label: string, availability: Availability, summary?: string): ComponentDef => ({
    id: 'code.' + id,
    label,
    kind: 'custom',
    availability,
    summary,
});

export const PALETTE: Category[] = [
    {
        id: 'sources',
        label: 'Sources',
        icon: '⬇',
        accent: '#7ee787',
        groups: [
            {
                id: 'src.files',
                label: 'Files',
                components: [
                    src('csv', 'CSV', 'available', 'Read delimited text files'),
                    src('tsv', 'TSV', 'planned'),
                    src('json', 'JSON', 'planned'),
                    src('jsonl', 'JSONL / NDJSON', 'planned'),
                    src('xml', 'XML', 'planned'),
                    src('excel', 'Excel (XLSX)', 'planned'),
                    src('avro', 'Avro', 'planned'),
                    src('parquet', 'Parquet', 'available', 'Read columnar Parquet files'),
                    src('orc', 'ORC', 'planned'),
                    src('fixedwidth', 'Fixed-width', 'planned'),
                    src('yaml', 'YAML', 'planned'),
                    src('toml', 'TOML', 'planned'),
                ],
            },
            {
                id: 'src.databases',
                label: 'Databases',
                components: [
                    src('postgres', 'PostgreSQL', 'planned'),
                    src('mysql', 'MySQL', 'planned'),
                    src('mariadb', 'MariaDB', 'planned'),
                    src('sqlserver', 'SQL Server', 'planned'),
                    src('oracle', 'Oracle', 'planned'),
                    src('db2', 'IBM DB2', 'planned'),
                    src('sqlite', 'SQLite', 'available', 'Read SQLite tables'),
                    src('duckdb', 'DuckDB', 'available', 'Read DuckDB tables'),
                    src('clickhouse', 'ClickHouse', 'planned'),
                    src('cockroach', 'CockroachDB', 'planned'),
                    src('jdbc', 'Generic JDBC', 'planned'),
                ],
            },
            {
                id: 'src.warehouses',
                label: 'Cloud Warehouses',
                components: [
                    src('snowflake', 'Snowflake', 'planned'),
                    src('bigquery', 'BigQuery', 'planned'),
                    src('redshift', 'Redshift', 'planned'),
                    src('databricks', 'Databricks SQL', 'planned'),
                    src('synapse', 'Azure Synapse', 'planned'),
                    src('motherduck', 'MotherDuck', 'planned'),
                ],
            },
            {
                id: 'src.storage',
                label: 'Object Storage',
                components: [
                    src('s3', 'Amazon S3', 'planned'),
                    src('gcs', 'Google Cloud Storage', 'planned'),
                    src('azureblob', 'Azure Blob Storage', 'planned'),
                    src('minio', 'MinIO', 'planned'),
                    src('r2', 'Cloudflare R2', 'planned'),
                    src('b2', 'Backblaze B2', 'planned'),
                ],
            },
            {
                id: 'src.streaming',
                label: 'Streaming',
                components: [
                    src('kafka', 'Apache Kafka', 'planned'),
                    src('pulsar', 'Apache Pulsar', 'planned'),
                    src('redpanda', 'Redpanda', 'planned'),
                    src('nats', 'NATS JetStream', 'planned'),
                    src('rabbit', 'RabbitMQ', 'planned'),
                    src('kinesis', 'AWS Kinesis', 'planned'),
                    src('eventhubs', 'Azure Event Hubs', 'planned'),
                    src('pubsub', 'GCP Pub/Sub', 'planned'),
                ],
            },
            {
                id: 'src.apis',
                label: 'APIs',
                components: [
                    src('rest', 'REST', 'planned'),
                    src('graphql', 'GraphQL', 'planned'),
                    src('grpc', 'gRPC', 'planned'),
                    src('webhook', 'Webhook', 'planned'),
                    src('soap', 'SOAP', 'planned'),
                    src('odata', 'OData', 'planned'),
                ],
            },
            {
                id: 'src.nosql',
                label: 'NoSQL & Search',
                components: [
                    src('mongodb', 'MongoDB', 'planned'),
                    src('cassandra', 'Cassandra', 'planned'),
                    src('scylla', 'ScyllaDB', 'planned'),
                    src('redis', 'Redis', 'planned'),
                    src('dynamodb', 'DynamoDB', 'planned'),
                    src('elastic', 'Elasticsearch', 'planned'),
                    src('opensearch', 'OpenSearch', 'planned'),
                    src('couchdb', 'CouchDB', 'planned'),
                ],
            },
            {
                id: 'src.misc',
                label: 'Other',
                components: [
                    src('ftp', 'SFTP / FTP', 'planned'),
                    src('http', 'HTTP', 'planned'),
                    src('email', 'Email (IMAP)', 'planned'),
                    src('git', 'Git Repository', 'planned'),
                    src('clipboard', 'Clipboard', 'planned'),
                ],
            },
        ],
    },
    {
        id: 'transforms',
        label: 'Transforms',
        icon: '∼',
        accent: '#58a6ff',
        groups: [
            {
                id: 'xf.fields',
                label: 'Fields',
                components: [
                    xf('map', 'Map', 'available', 'Talend tMap-style row mapper'),
                    xf('project', 'Project / Select', 'available'),
                    xf('cast', 'Cast / Convert Type', 'planned'),
                    xf('rename', 'Rename Columns', 'planned'),
                    xf('addcol', 'Add Column', 'planned'),
                    xf('dropcol', 'Drop Columns', 'planned'),
                    xf('reorder', 'Reorder Columns', 'planned'),
                    xf('coalesce', 'Coalesce / Null Fill', 'planned'),
                ],
            },
            {
                id: 'xf.rows',
                label: 'Rows',
                components: [
                    xf('filter', 'Filter Rows', 'available', 'WHERE-style row filter'),
                    xf('distinct', 'Distinct', 'planned'),
                    xf('sample', 'Sample', 'planned'),
                    xf('topn', 'Top N / Limit', 'planned'),
                    xf('sort', 'Sort', 'planned'),
                    xf('skip', 'Skip / Offset', 'planned'),
                ],
            },
            {
                id: 'xf.aggregate',
                label: 'Aggregate',
                components: [
                    xf('groupby', 'Group By', 'available'),
                    xf('rollup', 'Rollup', 'planned'),
                    xf('cube', 'Cube', 'planned'),
                    xf('aggwin', 'Window Aggregate', 'planned'),
                    xf('count', 'Count Rows', 'planned'),
                ],
            },
            {
                id: 'xf.join',
                label: 'Join',
                components: [
                    xf('join.inner', 'Inner Join', 'planned'),
                    xf('join.left', 'Left Join', 'planned'),
                    xf('join.right', 'Right Join', 'planned'),
                    xf('join.full', 'Full Outer Join', 'planned'),
                    xf('join.cross', 'Cross Join', 'planned'),
                    xf('lookup', 'Lookup', 'planned'),
                    xf('semi', 'Semi Join', 'planned'),
                    xf('anti', 'Anti Join', 'planned'),
                ],
            },
            {
                id: 'xf.set',
                label: 'Set Operations',
                components: [
                    xf('union', 'Union', 'planned'),
                    xf('unionall', 'Union All', 'planned'),
                    xf('intersect', 'Intersect', 'planned'),
                    xf('except', 'Except / Minus', 'planned'),
                ],
            },
            {
                id: 'xf.window',
                label: 'Window',
                components: [
                    xf('rownum', 'Row Number', 'planned'),
                    xf('rank', 'Rank', 'planned'),
                    xf('denserank', 'Dense Rank', 'planned'),
                    xf('lead', 'Lead', 'planned'),
                    xf('lag', 'Lag', 'planned'),
                    xf('first', 'First Value', 'planned'),
                    xf('last', 'Last Value', 'planned'),
                    xf('ntile', 'NTile', 'planned'),
                ],
            },
            {
                id: 'xf.strings',
                label: 'Strings',
                components: [
                    xf('regex', 'Regex Replace', 'planned'),
                    xf('split', 'Split', 'planned'),
                    xf('concat', 'Concat', 'planned'),
                    xf('trim', 'Trim / Pad', 'planned'),
                    xf('case', 'Case Change', 'planned'),
                    xf('length', 'Length', 'planned'),
                    xf('substring', 'Substring', 'planned'),
                    xf('format', 'Format String', 'planned'),
                ],
            },
            {
                id: 'xf.datetime',
                label: 'Date / Time',
                components: [
                    xf('dt.parse', 'Parse Date', 'planned'),
                    xf('dt.format', 'Format Date', 'planned'),
                    xf('dt.extract', 'Extract Part', 'planned'),
                    xf('dt.diff', 'Date Diff', 'planned'),
                    xf('dt.add', 'Date Add', 'planned'),
                    xf('dt.trunc', 'Truncate', 'planned'),
                    xf('dt.tz', 'Timezone Convert', 'planned'),
                ],
            },
            {
                id: 'xf.numeric',
                label: 'Numeric',
                components: [
                    xf('num.round', 'Round', 'planned'),
                    xf('num.mod', 'Modulo', 'planned'),
                    xf('num.abs', 'Absolute', 'planned'),
                    xf('num.log', 'Logarithm', 'planned'),
                    xf('num.power', 'Power', 'planned'),
                    xf('num.sqrt', 'Square Root', 'planned'),
                ],
            },
            {
                id: 'xf.pivot',
                label: 'Pivot / Shape',
                components: [
                    xf('pivot', 'Pivot', 'planned'),
                    xf('unpivot', 'Unpivot', 'planned'),
                    xf('denorm', 'Denormalize', 'planned'),
                    xf('norm', 'Normalize', 'planned'),
                    xf('transpose', 'Transpose', 'planned'),
                ],
            },
            {
                id: 'xf.json',
                label: 'JSON / Nested',
                components: [
                    xf('json.parse', 'Parse JSON', 'planned'),
                    xf('json.stringify', 'Stringify JSON', 'planned'),
                    xf('json.flatten', 'Flatten', 'planned'),
                    xf('json.path', 'JSONPath Extract', 'planned'),
                    xf('json.merge', 'Merge Objects', 'planned'),
                ],
            },
            {
                id: 'xf.array',
                label: 'Array',
                components: [
                    xf('arr.explode', 'Explode / Unnest', 'planned'),
                    xf('arr.collect', 'Collect List', 'planned'),
                    xf('arr.element', 'Element At', 'planned'),
                    xf('arr.contains', 'Contains', 'planned'),
                    xf('arr.distinct', 'Array Distinct', 'planned'),
                ],
            },
            {
                id: 'xf.cdc',
                label: 'CDC / SCD',
                components: [
                    xf('cdc.diff', 'Diff Detect', 'planned'),
                    xf('cdc.scd1', 'SCD Type 1', 'planned'),
                    xf('cdc.scd2', 'SCD Type 2', 'planned'),
                    xf('cdc.upsert', 'Merge / Upsert', 'planned'),
                ],
            },
        ],
    },
    {
        id: 'sinks',
        label: 'Sinks',
        icon: '⬆',
        accent: '#ffa657',
        groups: [
            {
                id: 'snk.files',
                label: 'Files',
                components: [
                    snk('csv', 'CSV', 'available'),
                    snk('tsv', 'TSV', 'planned'),
                    snk('json', 'JSON', 'planned'),
                    snk('jsonl', 'JSONL / NDJSON', 'planned'),
                    snk('xml', 'XML', 'planned'),
                    snk('excel', 'Excel (XLSX)', 'planned'),
                    snk('parquet', 'Parquet', 'available'),
                    snk('avro', 'Avro', 'planned'),
                    snk('orc', 'ORC', 'planned'),
                ],
            },
            {
                id: 'snk.databases',
                label: 'Databases',
                components: [
                    snk('postgres', 'PostgreSQL', 'planned'),
                    snk('mysql', 'MySQL', 'planned'),
                    snk('sqlserver', 'SQL Server', 'planned'),
                    snk('oracle', 'Oracle', 'planned'),
                    snk('sqlite', 'SQLite', 'available'),
                    snk('duckdb', 'DuckDB', 'available'),
                    snk('clickhouse', 'ClickHouse', 'planned'),
                    snk('jdbc', 'Generic JDBC', 'planned'),
                ],
            },
            {
                id: 'snk.warehouses',
                label: 'Cloud Warehouses',
                components: [
                    snk('snowflake', 'Snowflake', 'planned'),
                    snk('bigquery', 'BigQuery', 'planned'),
                    snk('redshift', 'Redshift', 'planned'),
                    snk('databricks', 'Databricks SQL', 'planned'),
                ],
            },
            {
                id: 'snk.storage',
                label: 'Object Storage',
                components: [
                    snk('s3', 'Amazon S3', 'planned'),
                    snk('gcs', 'Google Cloud Storage', 'planned'),
                    snk('azureblob', 'Azure Blob Storage', 'planned'),
                ],
            },
            {
                id: 'snk.streaming',
                label: 'Streaming',
                components: [
                    snk('kafka', 'Apache Kafka', 'planned'),
                    snk('pulsar', 'Apache Pulsar', 'planned'),
                    snk('nats', 'NATS JetStream', 'planned'),
                    snk('kinesis', 'AWS Kinesis', 'planned'),
                ],
            },
            {
                id: 'snk.apis',
                label: 'APIs',
                components: [
                    snk('rest', 'REST', 'planned'),
                    snk('webhook', 'Webhook', 'planned'),
                    snk('graphql', 'GraphQL Mutation', 'planned'),
                ],
            },
            {
                id: 'snk.nosql',
                label: 'NoSQL & Search',
                components: [
                    snk('mongodb', 'MongoDB', 'planned'),
                    snk('redis', 'Redis', 'planned'),
                    snk('elastic', 'Elasticsearch', 'planned'),
                    snk('opensearch', 'OpenSearch', 'planned'),
                ],
            },
        ],
    },
    {
        id: 'control',
        label: 'Control Flow',
        icon: '◇',
        accent: '#c39bff',
        groups: [
            {
                id: 'ctl.routing',
                label: 'Routing',
                components: [
                    ctl('replicate', 'Replicate / Tee', 'planned'),
                    ctl('switch', 'Switch / Conditional Split', 'planned'),
                    ctl('merge', 'Merge Streams', 'planned'),
                    ctl('iterate', 'Iterate', 'planned'),
                    ctl('foreach', 'For Each', 'planned'),
                ],
            },
            {
                id: 'ctl.timing',
                label: 'Timing',
                components: [
                    ctl('wait', 'Wait / Delay', 'planned'),
                    ctl('schedule', 'Schedule', 'planned'),
                    ctl('throttle', 'Throttle', 'planned'),
                ],
            },
            {
                id: 'ctl.pipeline',
                label: 'Pipelines',
                components: [
                    ctl('runpipeline', 'Run Pipeline', 'planned'),
                    ctl('trigger', 'Trigger Pipeline', 'planned'),
                    ctl('checkpoint', 'Checkpoint', 'planned'),
                ],
            },
            {
                id: 'ctl.errors',
                label: 'Error Handling',
                components: [
                    ctl('try', 'Try / Catch', 'planned'),
                    ctl('retry', 'Retry', 'planned'),
                    ctl('deadletter', 'Dead Letter Queue', 'planned'),
                ],
            },
        ],
    },
    {
        id: 'quality',
        label: 'Data Quality',
        icon: '✓',
        accent: '#fed060',
        groups: [
            {
                id: 'qa.validation',
                label: 'Validation',
                components: [
                    qa('schemavalidate', 'Schema Validate', 'planned'),
                    qa('regex', 'Regex Match', 'planned'),
                    qa('range', 'Range Check', 'planned'),
                    qa('notnull', 'Not-Null Check', 'planned'),
                    qa('unique', 'Uniqueness Check', 'planned'),
                ],
            },
            {
                id: 'qa.profile',
                label: 'Profiling',
                components: [
                    qa('profile', 'Column Profile', 'planned'),
                    qa('describe', 'Describe', 'planned'),
                    qa('histogram', 'Histogram', 'planned'),
                ],
            },
            {
                id: 'qa.cleanse',
                label: 'Cleansing',
                components: [
                    qa('standardize', 'Standardize', 'planned'),
                    qa('dedupe', 'Fuzzy Deduplicate', 'planned'),
                    qa('match', 'Record Match', 'planned'),
                    qa('addressclean', 'Address Cleanse', 'planned'),
                ],
            },
        ],
    },
    {
        id: 'code',
        label: 'Custom Code',
        icon: '{ }',
        accent: '#ff7b72',
        groups: [
            {
                id: 'code.sql',
                label: 'SQL',
                components: [
                    code('sql', 'Inline SQL', 'planned'),
                    code('sqltemplate', 'SQL Template', 'planned'),
                ],
            },
            {
                id: 'code.scripts',
                label: 'Scripting',
                components: [
                    code('python', 'Python UDF', 'planned'),
                    code('rust', 'Rust UDF', 'planned'),
                    code('javascript', 'JavaScript UDF', 'planned'),
                    code('shell', 'Shell Command', 'planned'),
                    code('wasm', 'WebAssembly UDF', 'planned'),
                ],
            },
        ],
    },
    {
        id: 'saas',
        label: 'SaaS Connectors',
        icon: '☁',
        accent: '#8b949e',
        groups: [
            {
                id: 'saas.crm',
                label: 'CRM',
                components: [
                    src('salesforce', 'Salesforce', 'planned'),
                    src('hubspot', 'HubSpot', 'planned'),
                    src('pipedrive', 'Pipedrive', 'planned'),
                ],
            },
            {
                id: 'saas.finance',
                label: 'Finance',
                components: [
                    src('stripe', 'Stripe', 'planned'),
                    src('quickbooks', 'QuickBooks', 'planned'),
                    src('xero', 'Xero', 'planned'),
                ],
            },
            {
                id: 'saas.productivity',
                label: 'Productivity',
                components: [
                    src('notion', 'Notion', 'planned'),
                    src('airtable', 'Airtable', 'planned'),
                    src('gsheets', 'Google Sheets', 'planned'),
                    src('excel-online', 'Microsoft Excel Online', 'planned'),
                ],
            },
            {
                id: 'saas.devtools',
                label: 'Dev Tools',
                components: [
                    src('github', 'GitHub', 'planned'),
                    src('gitlab', 'GitLab', 'planned'),
                    src('linear', 'Linear', 'planned'),
                    src('jira', 'Jira', 'planned'),
                ],
            },
            {
                id: 'saas.marketing',
                label: 'Marketing',
                components: [
                    src('mailchimp', 'Mailchimp', 'planned'),
                    src('sendgrid', 'SendGrid', 'planned'),
                    src('segment', 'Segment', 'planned'),
                ],
            },
        ],
    },
];

export const ALL_COMPONENTS: ComponentDef[] = PALETTE.flatMap(c => c.groups.flatMap(g => g.components));

export const TOTAL_COMPONENT_COUNT = ALL_COMPONENTS.length;

export const AVAILABLE_COUNT = ALL_COMPONENTS.filter(c => c.availability === 'available').length;
