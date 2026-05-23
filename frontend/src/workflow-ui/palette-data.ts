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
                    src('tsv', 'TSV', 'available', 'Read tab-separated files'),
                    src('json', 'JSON', 'available', 'Read JSON files'),
                    src('jsonl', 'JSONL / NDJSON', 'available', 'Read newline-delimited JSON'),
                    src('xml', 'XML', 'planned'),
                    src('excel', 'Excel (XLSX)', 'available', 'Read .xlsx via the DuckDB excel extension'),
                    src('avro', 'Avro', 'available', 'Read Avro files via the DuckDB avro community extension'),
                    src('parquet', 'Parquet', 'available', 'Read columnar Parquet files'),
                    src('orc', 'ORC', 'planned'),
                    src('fixedwidth', 'Fixed-width', 'planned'),
                    src('yaml', 'YAML', 'planned'),
                    src('toml', 'TOML', 'planned'),
                ],
            },
            {
                id: 'src.lakehouse',
                label: 'Lakehouse table formats',
                components: [
                    src('iceberg', 'Apache Iceberg', 'available', 'Read Iceberg tables via DuckDB iceberg_scan'),
                    src('delta', 'Delta Lake', 'available', 'Read Delta Lake tables via DuckDB delta_scan'),
                ],
            },
            {
                id: 'src.databases',
                label: 'Databases',
                components: [
                    src('postgres', 'PostgreSQL', 'available', 'Read from PostgreSQL via the DuckDB postgres extension'),
                    src('mysql', 'MySQL', 'available', 'Read from MySQL via the DuckDB mysql extension'),
                    src('mariadb', 'MariaDB', 'available', 'Read from MariaDB via the DuckDB mysql extension'),
                    src('sqlserver', 'SQL Server', 'planned'),
                    src('oracle', 'Oracle', 'planned'),
                    src('db2', 'IBM DB2', 'planned'),
                    src('sqlite', 'SQLite', 'available', 'Read SQLite tables'),
                    src('duckdb', 'DuckDB', 'available', 'Read a table from a DuckDB file'),
                    src('clickhouse', 'ClickHouse', 'planned'),
                    src('cockroach', 'CockroachDB', 'available', 'Read from CockroachDB via the DuckDB postgres extension'),
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
                    src('motherduck', 'MotherDuck', 'available', 'Read from MotherDuck via ATTACH md:'),
                ],
            },
            {
                id: 'src.storage',
                label: 'Object Storage',
                components: [
                    src('s3', 'Amazon S3', 'available', 'Read via DuckDB httpfs'),
                    src('gcs', 'Google Cloud Storage', 'available', 'Read via DuckDB httpfs'),
                    src('azureblob', 'Azure Blob Storage', 'available', 'Read via the azure extension'),
                    src('minio', 'MinIO', 'available', 'Read via S3-compatible endpoint'),
                    src('r2', 'Cloudflare R2', 'available', 'Read via S3-compatible endpoint'),
                    src('b2', 'Backblaze B2', 'available', 'Read via S3-compatible endpoint'),
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
            {
                id: 'src.vector',
                label: 'Vector / AI Databases',
                components: [
                    src('pgvector', 'pgvector (Postgres)', 'preview', 'Read embeddings + metadata'),
                    src('pinecone', 'Pinecone', 'preview', 'Fetch or similarity-search vectors'),
                    src('qdrant', 'Qdrant', 'preview'),
                    src('weaviate', 'Weaviate', 'preview'),
                    src('chroma', 'Chroma', 'preview'),
                    src('milvus', 'Milvus', 'preview'),
                    src('lancedb', 'LanceDB', 'preview'),
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
                    xf('map', 'Map', 'available', 'Visual row mapper with main + lookup inputs'),
                    xf('project', 'Project / Select', 'available'),
                    xf('cast', 'Cast / Convert Type', 'available'),
                    xf('rename', 'Rename Columns', 'available'),
                    xf('addcol', 'Add Column', 'available'),
                    xf('dropcol', 'Drop Columns', 'available'),
                    xf('reorder', 'Reorder Columns', 'available'),
                    xf('coalesce', 'Coalesce / Null Fill', 'available', 'Fill nulls via an expression'),
                ],
            },
            {
                id: 'xf.rows',
                label: 'Rows',
                components: [
                    xf('filter', 'Filter Rows', 'available', 'WHERE-style row filter'),
                    xf('distinct', 'Distinct', 'available', 'Drop duplicate rows'),
                    xf('sample', 'Sample', 'available', 'Random row sample'),
                    xf('topn', 'Top N / Limit', 'available', 'Keep the first N rows'),
                    xf('sort', 'Sort', 'available', 'Order rows'),
                    xf('skip', 'Skip / Offset', 'available', 'Drop the first N rows'),
                ],
            },
            {
                id: 'xf.aggregate',
                label: 'Aggregate',
                components: [
                    xf('groupby', 'Group By', 'available'),
                    xf('rollup', 'Rollup', 'available'),
                    xf('cube', 'Cube', 'available'),
                    xf('aggwin', 'Window Aggregate', 'available', 'Aggregate over a window, keep every row'),
                    xf('count', 'Count Rows', 'available'),
                ],
            },
            {
                id: 'xf.join',
                label: 'Join',
                components: [
                    xf('join.inner', 'Inner Join', 'available'),
                    xf('join.left', 'Left Join', 'available'),
                    xf('join.right', 'Right Join', 'available'),
                    xf('join.full', 'Full Outer Join', 'available'),
                    xf('join.cross', 'Cross Join', 'available'),
                    xf('lookup', 'Lookup', 'available'),
                    xf('semi', 'Semi Join', 'available'),
                    xf('anti', 'Anti Join', 'available'),
                ],
            },
            {
                id: 'xf.set',
                label: 'Set Operations',
                components: [
                    xf('union', 'Union', 'available', 'Combine inputs, drop duplicates'),
                    xf('unionall', 'Union All', 'available', 'Combine inputs, keep all rows'),
                    xf('intersect', 'Intersect', 'available', 'Rows present in all inputs'),
                    xf('except', 'Except / Minus', 'available', 'Rows in the first input only'),
                ],
            },
            {
                id: 'xf.window',
                label: 'Window',
                components: [
                    xf('rownum', 'Row Number', 'available', 'ROW_NUMBER() over a window'),
                    xf('rank', 'Rank', 'available'),
                    xf('denserank', 'Dense Rank', 'available'),
                    xf('lead', 'Lead', 'available'),
                    xf('lag', 'Lag', 'available'),
                    xf('first', 'First Value', 'available'),
                    xf('last', 'Last Value', 'available'),
                    xf('ntile', 'NTile', 'available'),
                ],
            },
            {
                id: 'xf.strings',
                label: 'Strings',
                components: [
                    xf('regex', 'Regex Replace', 'available'),
                    xf('split', 'Split', 'available'),
                    xf('concat', 'Concat', 'available'),
                    xf('trim', 'Trim', 'available'),
                    xf('case', 'Case Change', 'available'),
                    xf('length', 'Length', 'available'),
                    xf('substring', 'Substring', 'available'),
                    xf('format', 'Format String', 'available'),
                ],
            },
            {
                id: 'xf.datetime',
                label: 'Date / Time',
                components: [
                    xf('dt.parse', 'Parse Date', 'available'),
                    xf('dt.format', 'Format Date', 'available'),
                    xf('dt.extract', 'Extract Part', 'available'),
                    xf('dt.diff', 'Date Diff', 'available'),
                    xf('dt.add', 'Date Add', 'available'),
                    xf('dt.trunc', 'Truncate', 'available'),
                    xf('dt.tz', 'Timezone Convert', 'available'),
                ],
            },
            {
                id: 'xf.numeric',
                label: 'Numeric',
                components: [
                    xf('num.round', 'Round', 'available'),
                    xf('num.mod', 'Modulo', 'available'),
                    xf('num.abs', 'Absolute', 'available'),
                    xf('num.log', 'Logarithm', 'available'),
                    xf('num.power', 'Power', 'available'),
                    xf('num.sqrt', 'Square Root', 'available'),
                ],
            },
            {
                id: 'xf.pivot',
                label: 'Pivot / Shape',
                components: [
                    xf('pivot', 'Pivot', 'available', 'Rows to columns'),
                    xf('unpivot', 'Unpivot', 'available', 'Columns to name/value rows (wide to long)'),
                    xf('denorm', 'Denormalize', 'available', 'Collapse rows per group, joining columns into delimited cells'),
                    xf('norm', 'Normalize', 'available', 'Explode a delimited or array column into rows'),
                    xf('transpose', 'Transpose', 'available', 'Swap rows and columns'),
                ],
            },
            {
                id: 'xf.json',
                label: 'JSON / Nested',
                components: [
                    xf('json.parse', 'Parse JSON', 'available'),
                    xf('json.stringify', 'Stringify JSON', 'available'),
                    xf('json.flatten', 'Flatten', 'available'),
                    xf('json.path', 'JSONPath Extract', 'available'),
                    xf('json.merge', 'Merge Objects', 'available'),
                ],
            },
            {
                id: 'xf.array',
                label: 'Array',
                components: [
                    xf('arr.explode', 'Explode / Unnest', 'available'),
                    xf('arr.collect', 'Collect List', 'available'),
                    xf('arr.element', 'Element At', 'available'),
                    xf('arr.contains', 'Contains', 'available'),
                    xf('arr.distinct', 'Array Distinct', 'available'),
                ],
            },
            {
                id: 'xf.cdc',
                label: 'CDC / SCD',
                components: [
                    xf('cdc.diff', 'Diff Detect', 'available', 'Tag inserted/updated/deleted rows vs a previous snapshot'),
                    xf('cdc.scd1', 'SCD Type 1', 'available', 'Resolved current state: cur + prev rows whose key is not in cur'),
                    xf('cdc.scd2', 'SCD Type 2', 'available', 'Maintain versioned history: close changed rows, insert new versions'),
                    xf('cdc.upsert', 'Merge / Upsert', 'available', 'Emit the upsert payload: new + changed rows from cur'),
                ],
            },
            {
                id: 'xf.ai',
                label: 'AI',
                components: [
                    xf('ai.embed', 'Embeddings', 'preview', 'Generate vector embeddings'),
                    xf('ai.llm', 'LLM Transform', 'preview', 'Clean / enrich rows with an LLM'),
                    xf('ai.chunk', 'Text Chunker', 'preview', 'Split text for RAG'),
                    xf('ai.pii', 'PII Redact', 'preview', 'Detect + mask personal data'),
                    xf('ai.classify', 'Classify', 'preview', 'Label rows with a model'),
                    xf('ai.dedupe', 'Semantic Dedupe', 'preview', 'Drop near-duplicate rows'),
                ],
            },
            {
                id: 'xf.debug',
                label: 'Debug',
                components: [
                    xf('log', 'Log Rows', 'available', 'Pass rows through and print them to Output'),
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
                    snk('json', 'JSON', 'available'),
                    snk('jsonl', 'JSONL / NDJSON', 'available'),
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
                    snk('postgres', 'PostgreSQL', 'available', 'Write to PostgreSQL via the DuckDB postgres extension'),
                    snk('mysql', 'MySQL', 'available', 'Write to MySQL via the DuckDB mysql extension'),
                    snk('sqlserver', 'SQL Server', 'planned'),
                    snk('oracle', 'Oracle', 'planned'),
                    snk('sqlite', 'SQLite', 'available', 'Write a table into a SQLite file'),
                    snk('duckdb', 'DuckDB', 'available', 'Write a table into a DuckDB file'),
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
                    snk('s3', 'Amazon S3', 'available', 'Write via DuckDB httpfs'),
                    snk('gcs', 'Google Cloud Storage', 'available', 'Write via DuckDB httpfs'),
                    snk('azureblob', 'Azure Blob Storage', 'available', 'Write via the azure extension'),
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
            {
                id: 'snk.vector',
                label: 'Vector / AI Databases',
                components: [
                    snk('pgvector', 'pgvector (Postgres)', 'preview', 'Write embeddings to Postgres + pgvector'),
                    snk('pinecone', 'Pinecone', 'preview', 'Upsert vectors + metadata'),
                    snk('qdrant', 'Qdrant', 'preview'),
                    snk('weaviate', 'Weaviate', 'preview'),
                    snk('chroma', 'Chroma', 'preview'),
                    snk('milvus', 'Milvus', 'preview'),
                    snk('lancedb', 'LanceDB', 'preview'),
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
                    ctl('replicate', 'Replicate / Tee', 'available', 'Send the same data to multiple downstream outputs'),
                    ctl('switch', 'Switch / Conditional Split', 'available', 'Route rows to case_1..N outputs by condition; first match wins'),
                    ctl('merge', 'Merge Streams', 'available', 'Concatenate multiple input streams (UNION ALL)'),
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
                    qa('schemavalidate', 'Schema Validate', 'available', 'Reject rows where any expected column is null'),
                    qa('regex', 'Regex Match', 'available', 'Pass rows matching a pattern; rest to reject'),
                    qa('range', 'Range Check', 'available', 'Pass in-range rows; rest to reject'),
                    qa('notnull', 'Not-Null Check', 'available', 'Pass rows with no nulls; rest to reject'),
                    qa('unique', 'Uniqueness Check', 'available', 'Pass first per key; duplicates to reject'),
                ],
            },
            {
                id: 'qa.profile',
                label: 'Profiling',
                components: [
                    qa('profile', 'Column Profile', 'available', 'Per-column stats: count, nulls, distinct, min/max, quartiles'),
                    qa('describe', 'Describe', 'available', 'Column names and types of the input'),
                    qa('histogram', 'Histogram', 'available', 'Value frequencies for a column'),
                ],
            },
            {
                id: 'qa.cleanse',
                label: 'Cleansing',
                components: [
                    qa('standardize', 'Standardize', 'available', 'Trim, case-normalize, and collapse whitespace'),
                    qa('dedupe', 'Fuzzy Deduplicate', 'available', 'Drop near-duplicate rows by string similarity'),
                    qa('match', 'Record Match', 'available', 'Find matching record pairs by similarity, with a score'),
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
                    code('sql', 'Inline SQL', 'available', 'Run a SELECT; upstream is `input`'),
                    code('sqltemplate', 'SQL Template', 'available', 'Parameterized SQL with ${context.var}'),
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
