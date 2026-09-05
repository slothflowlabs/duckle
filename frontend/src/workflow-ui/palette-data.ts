export type Availability = 'available' | 'planned' | 'preview';

export type NodeKind = 'source' | 'transform' | 'sink' | 'control' | 'quality' | 'custom';

export type ComponentDef = {
    id: string;
    label: string;
    kind: NodeKind;
    availability: Availability;
    summary?: string;
    /** When set, shown in the palette for preview/planned tiles. */
    alternateHint?: string;
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

const src = (
    id: string,
    label: string,
    availability: Availability,
    summary?: string,
    alternateHint?: string,
): ComponentDef => ({
    id: 'src.' + id,
    label,
    kind: 'source',
    availability,
    summary,
    alternateHint,
});

const snk = (id: string, label: string, availability: Availability, summary?: string): ComponentDef => ({
    id: 'snk.' + id,
    label,
    kind: 'sink',
    availability,
    summary,
});

const xf = (
    id: string,
    label: string,
    availability: Availability,
    summary?: string,
    alternateHint?: string,
): ComponentDef => ({
    id: 'xf.' + id,
    label,
    kind: 'transform',
    availability,
    summary,
    alternateHint,
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
        accent: '#2eafff',
        groups: [
            {
                id: 'src.files',
                label: 'Files',
                components: [
                    src('csv', 'CSV', 'available', 'Read delimited text files'),
                    src('tsv', 'TSV', 'available', 'Read tab-separated files'),
                    src('json', 'JSON', 'available', 'Read JSON files'),
                    src('jsonl', 'JSONL / NDJSON', 'available', 'Read newline-delimited JSON'),
                    src('changed', 'Changed? (remote poll)', 'available', 'Poll a remote source METADATA and emit a row only for what changed - a HEAD or an SFTP stat costs nothing next to the object it decides about. Object mode watches one URI; listing mode watches an s3:// prefix or an sftp:// directory of immutable files and emits the new and changed ones for a ForEach downstream. Emits uri / name / size / modified_at / etag / fingerprint / status. When nothing changed the node reports `unchanged` rather than a bare success, so a working poll and a broken one are told apart. Fingerprints are conservative: a missing or unreadable signal counts as CHANGED, because re-reading costs compute and skipping loses data. Position advances only when the run succeeds.'),
                    src('ducklake.maintain', 'DuckLake Maintenance', 'available', 'Run one of the maintenance operations DuckLake itself provides and emit what it did as ordinary rows. Compact small files, rewrite files heavy with deletes, expire snapshots, clean up files an expired snapshot released, delete orphaned files, flush inlined data, or read per-table storage statistics. Deliberately thin: each operation is one DuckLake function and its options are the options of that function, so nothing here invents storage semantics. The three destructive operations support DRY RUN, which lists exactly what would go and changes nothing; ticking it on an operation DuckLake cannot dry-run is REFUSED rather than ignored. Snapshot expiry does nothing without an explicit retention boundary. Two maintenance runs against one catalog serialise on a lock rather than racing.'),
                    src('spool', 'Spool (tail NDJSON)', 'available', 'Tail an append-only NDJSON file from where the last SUCCESSFUL run stopped, by byte offset. Pairs with `duckle-runner listen`, which keeps a webhook listener up and appends here - so nothing is lost between pipeline runs, unlike src.webhook which only collects while a run is executing. A failed run leaves the position alone, so those records are re-read rather than dropped.'),
                    src('model', 'Model Card', 'available', 'Read a registered model card back as one row: name, version, the artifact URI the training script wrote, and whatever metrics and hashes it recorded. Address it as name@version, or name@latest to follow the pointer that moves on every successful retrain, so a scoring pipeline stays unedited. The engine never loads the model - the row carries the URI and your Python stage loads it.'),
                    src('pdf', 'PDF Pages', 'available', 'One row per PAGE of a PDF: document_id, page_number, text, has_text_layer, width, height and the document metadata. Point it at a file or a folder. Reads the text layer a document already carries, so filings, accounts and invoices become a table you can filter, join and hand to a Python or AI stage. No OCR: a scanned page comes back with has_text_layer false, which is exactly what lets you route those pages to whatever OCR you already run. document_id is the same value src.artifact puts in uri, so the two join.'),
                    src('html', 'HTML', 'available', 'Rows out of an HTML page, by CSS selector. Point it at a local file or an http(s) URL, give a row selector, and either name a column per sub-selector (with an optional attribute, so a link href or a data- value is readable) or leave the columns empty and let a table become a table: the th cells name the columns and each tr is a row. Parsed with a tolerant HTML parser, so the unclosed tags and unquoted attributes that real pages carry - and that the strict XML reader rejects outright - are fine.'),
                    src('xml', 'XML', 'available', 'Read XML files via the pure-Rust `quick-xml` parser. rowPath is a slash-separated element walk (e.g. `library/books/book`); every matching element becomes one row. Attributes prefix with `@`, text content goes to `_text`, nested children nest; repeated same-name siblings collapse to arrays.'),
                    src('excel', 'Excel (XLSX)', 'available', 'Read .xlsx via the DuckDB excel extension'),
                    src('avro', 'Avro', 'available', 'Apache Avro container files (.avro / .ocf) via the pure-Rust `apache-avro` crate. The file carries its own schema; engine doesn\'t need any schema config. Pairs with Kafka topics that publish Avro-encoded payloads.'),
                    src('qvd', 'Qlik QVD', 'available', 'Qlik QVD files (.qvd) via a clean-room pure-Rust reader (no Qlik runtime). The QVD header carries its own schema; the symbol table + bit-stuffed index are decoded directly. Move QlikView / Qlik Sense extracts into DuckDB, Parquet or any sink.'),
                    src('parquet', 'Parquet', 'available', 'Read columnar Parquet files'),
                    src('vortex', 'Vortex', 'available', 'Read Vortex columnar files (.vortex) via the bundled duckle-lance sidecar. Vortex is a next-gen columnar format with fast random access; the sidecar bridges it into the engine through Parquet.'),
                    src('orc', 'ORC', 'planned'),
                    src('fixedwidth', 'Fixed-width', 'available', 'Read positional / fixed-width text files (mainframe / banking exports). Form provides a columns array - {name, start (1-based), width}; engine builds SUBSTR projections. Trailing whitespace stripped by default.'),
                    src('yaml', 'YAML', 'available', 'Read a YAML file as a table. Top-level YAML arrays become one row per element; non-array docs become a single row. Suits config-data ETL (Helm values, GitHub Actions matrices) not bulk logs.'),
                    src('toml', 'TOML', 'available', 'Read a TOML file as a table. Top-level TOML doc becomes one row (TOML disallows a top-level array). Suits Cargo / pyproject / Hugo config audits.'),
                    src('spatial', 'Geospatial (GeoParquet / GeoJSON / Shapefile / GeoPackage)', 'available', 'Read geospatial files: GeoParquet natively, and GeoJSON / Shapefile / GeoPackage / KML / GPX / GML via the DuckDB spatial extension (ST_Read)'),
                    src('gdb', 'Esri File Geodatabase (.gdb)', 'available', 'Read a feature class (layer) from an Esri File Geodatabase via the spatial extension (ST_Read with layer=)'),
                    src('huggingface', 'Hugging Face dataset', 'available', 'Read a Hugging Face Hub dataset directly via DuckDB hf:// (httpfs). Give the repo id and a file/glob; CSV / JSON / Parquet auto-detected. Token for private or gated datasets.'),
                ],
            },
            {
                id: 'src.lakehouse',
                label: 'Lakehouse table formats',
                components: [
                    src('iceberg', 'Apache Iceberg', 'available', 'Read Iceberg tables via DuckDB iceberg_scan'),
                    src('delta', 'Delta Lake', 'available', 'Read Delta Lake tables via DuckDB delta_scan'),
                    src('ducklake', 'DuckLake', 'available', 'Read tables from a DuckLake catalog (DuckDB native lakehouse)'),
                    src('ducklake.changes', 'DuckLake CDC', 'available', 'Change-data-feed source: reads table_changes() since the last consumed snapshot (saved in workspace state), emitting row-level insert / delete / update_preimage / update_postimage with a change_type column. True incremental CDC for DuckLake-managed tables.'),
                    src('ducklake.diff', 'DuckLake Data Diff', 'available', 'Data diff between two snapshots of a DuckLake table: emits the row-level change feed (insert / delete / update_preimage / update_postimage with a change_type column) between a chosen From and To snapshot. Pick snapshots with Browse; wire into a validator to assert expected changes in CI.'),
                ],
            },
            {
                id: 'src.databases',
                label: 'Databases',
                components: [
                    src('postgres', 'PostgreSQL', 'available', 'Read from PostgreSQL via the DuckDB postgres extension'),
                    src('mysql', 'MySQL', 'available', 'Read from MySQL via the DuckDB mysql extension'),
                    src('mariadb', 'MariaDB', 'available', 'Read from MariaDB via the DuckDB mysql extension'),
                    src('sqlserver', 'SQL Server', 'available', 'Read SQL Server via the native TDS protocol (tiberius, pure Rust). SQL auth (user/password); trust_cert option for self-signed dev servers.'),
                    src('oracle', 'Oracle', 'available', 'Read Oracle via the official `oracle` Rust crate (ODPI-C). Built into the shipped binary - users need Oracle Instant Client (libclntsh.{so,dll,dylib}) on the library path at RUNTIME; the executor surfaces a clear OCI loader error if it\'s missing. SQL auth via user / password; EZ Connect string for host:port/service_name.'),
                    src('db2', 'IBM DB2', 'available', 'Read IBM DB2 through the IBM Data Server ODBC driver (DB2 ships no DuckDB extension and no native Rust driver). Install the IBM driver, then connect with friendly host / port / database / user / password fields, a DSN, or a full ODBC connection string. Whole-table read or custom SQL; types preserved.'),
                    src('sqlite', 'SQLite', 'available', 'Read SQLite tables'),
                    src('turso', 'Turso / libSQL', 'available', 'Read a Turso (libSQL) database over the HTTP pipeline API - no driver install. Paste the libsql:// URL the dashboard gives you (it is normalized to https) plus a database auth token. Whole-table read or custom SQL.'),
                    src('duckdb', 'DuckDB', 'available', 'Read a table from a DuckDB file'),
                    src('clickhouse', 'ClickHouse', 'available', 'Read ClickHouse via the HTTP interface (POST SELECT ... FORMAT JSON). User/password auth via X-ClickHouse-User / X-ClickHouse-Key headers.'),
                    src('cockroach', 'CockroachDB', 'available', 'Read from CockroachDB via the DuckDB postgres extension'),
                    src('adbc', 'ADBC (Arrow)', 'available', 'Read any database that ships an ADBC (Arrow Database Connectivity) driver. Point at a prebuilt driver shared library (.dll / .so / .dylib) plus a connection URI and SQL; rows stream back as Arrow for fast loads. Friendly wrappers can map their own fields onto driver / options.'),
                    src('teradata', 'Teradata', 'available', 'Read from Teradata through its free ODBC driver (no DuckDB Teradata extension exists). Install the Teradata ODBC driver, then connect with friendly host / user / password / database fields, a DSN, or a full ODBC connection string. Whole-table read or custom SQL; types preserved.'),
                    src('jdbc', 'Generic JDBC', 'planned'),
                ],
            },
            {
                id: 'src.warehouses',
                label: 'Cloud Warehouses',
                components: [
                    src('snowflake', 'Snowflake', 'available', 'Read Snowflake via the SQL API (/api/v2/statements). Supports PAT and JWT RS256 auth; engine materializes inline result sets as a DuckDB table for downstream stages.'),
                    src('bigquery', 'BigQuery', 'available', 'Read tables from BigQuery via the duckdb-bigquery community extension - uses standard GCP credential discovery'),
                    src('redshift', 'Redshift', 'available', 'Read Redshift via the postgres ATTACH path (Redshift speaks Postgres wire on port 5439)'),
                    src('databricks', 'Databricks SQL', 'available', 'Read Databricks via the SQL Statement Execution API with PAT Bearer auth. Engine materializes inline result sets as a DuckDB table for downstream stages.'),
                    src('synapse', 'Azure Synapse', 'available', 'Azure Synapse rides the SQL Server TDS wire - same connection form as src.sqlserver.'),
                    src('motherduck', 'MotherDuck', 'available', 'Read from MotherDuck via ATTACH md:'),
                    src('quack', 'DuckDB Quack', 'available', 'Read tables from a remote DuckDB instance over the Quack protocol (HTTP on port 9494). Server runs quack_serve(...); client ATTACHes the quack: URL with a token-based SECRET.'),
                    src('gizmosql', 'GizmoSQL', 'available', 'Query a GizmoSQL (Arrow Flight SQL) server via a clean-room pure-Rust Flight SQL client - no ADBC driver or JDBC needed. Basic-auth handshake then streams Arrow back for fast loads; TLS optional.'),
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
                    src('kafka', 'Apache Kafka', 'available', 'Batch-consume up to maxRecords messages from a single partition via the pure-Rust `rskafka` driver. Emits {offset, key, value, timestamp_ms} rows. startOffset negative = read from earliest available; positive = read from that offset. Batch ETL semantics - continuous streaming is on the roadmap.'),
                    src('pulsar', 'Apache Pulsar', 'planned'),
                    src('redpanda', 'Redpanda', 'available', 'Same wire protocol as Kafka - rides the rskafka driver. Use src.kafka semantics: batch-consume up to maxRecords from a single partition.'),
                    src('nats', 'NATS JetStream', 'available', 'Subscribe-with-timeout collector via the pure-Rust `async-nats` driver. Drains up to maxRecords messages from subject within timeoutMs wall-clock. Emits {subject, payload} rows. Batch ETL semantics - continuous streaming is on the roadmap.'),
                    src('rabbit', 'RabbitMQ', 'available', 'Pull messages from a queue via the pure-Rust `lapin` AMQP 0.9.1 driver. Polls until maxMessages or timeoutMs wall-clock elapses; auto-acks each pulled message. Emits {payload, routing_key, exchange, delivery_tag} rows.'),
                    src('kinesis', 'AWS Kinesis', 'available', 'Single-shard Kinesis read via direct HTTP + AWS SigV4 (no AWS SDK). Walks ListShards -> GetShardIterator -> GetRecords. Props: region, accessKeyId, secretAccessKey, sessionToken (optional STS), streamName, shardIndex (default 0), iteratorType (TRIM_HORIZON or LATEST), maxRecords. Records with JSON-object payloads unfold as rows; others land as {partition_key, sequence_number, data}. Multi-shard parallelism deferred.'),
                    src('eventhubs', 'Azure Event Hubs', 'planned'),
                    src('pubsub', 'GCP Pub/Sub', 'available', 'Pull messages via the Pub/Sub REST API (POST /v1/projects/{p}/subscriptions/{s}:pull) - sidesteps the gRPC build dependency. Auto-acks the batch. Auth via a pre-fetched OAuth2 Bearer access token (mint with `gcloud auth print-access-token`). Emits {message_id, publish_time, data} rows.'),
                    src('websocket', 'WebSocket', 'available', 'Connect to a ws:// or wss:// URL, optionally send a subscribe frame, and collect up to maxMessages frames (or until timeoutMs). JSON object -> one row, JSON array -> a row each, other text -> {message}. For live feeds (market data, sensor streams). Batch ETL semantics.'),
                ],
            },
            {
                id: 'src.apis',
                label: 'APIs',
                components: [
                    src('rest', 'REST', 'available', 'Generic HTTP GET/POST source. Parses JSON response, optionally walks a JSON pointer (responsePath) to find the row array, and follows cursor-style pagination if configured (cursorNextPath + cursorParam).'),
                    src('graphql', 'GraphQL', 'available', 'POST a GraphQL query to an endpoint and walk the response data path. Rides snk.rest/src.rest infrastructure; auth via Bearer / API-Key.'),
                    src('grpc', 'gRPC', 'planned'),
                    src('webhook', 'Webhook', 'available', 'Bind 127.0.0.1:port and collect up to `maxRequests` inbound HTTP requests with a global `timeoutMs` deadline. JSON-object bodies become the row; JSON-array bodies unfold into rows; other bodies fall back to {method, path, body, headers}. Local-only by design - point a tunnel (ngrok / cloudflared) at the port for public reach.'),
                    src('soap', 'SOAP', 'available', 'SOAP / generic XML-API source. Thin alias over src.rest with defaults: POST, Content-Type text/xml; charset=utf-8, responseFormat=xml. Set responsePath to the element-name walk into the body (e.g. Envelope/Body/GetUsersResponse/Users/User), supply the XML envelope in `body`, optionally add a `soapAction` prop for the SOAPAction header.'),
                    src('odata', 'OData', 'available', 'OData v4 source - thin alias over src.rest. Defaults: responsePath /value, pagination follows @odata.nextLink as a complete URL. Set authType (basic / bearer / apikey) on the form. Works with SAP, D365, Microsoft Graph, any OData v4 endpoint.'),
                    src('sap', 'SAP (OData / CDS)', 'available', 'SAP S/4HANA & ECC source over OData - covers OData services and CDS views published as OData (@OData.publish). Native HTTP, no SAP GUI or SDK. Set odataVersion (v2 classic Gateway = /d/results with __next paging; v4 RAP = /value with @odata.nextLink), sapClient (mandate, appended as sap-client=NNN), and authType (basic for the S-user, bearer for OAuth). $format=json is added automatically.'),
                    src('dhis2', 'DHIS2', 'available', 'DHIS2 Web API source - thin alias over src.rest. Auth: pick API key, set authHeader to Authorization, and put "ApiToken d2pat_..." (2.37+) or "Basic <user:password>" in the token field; plain Basic works too. Set responsePath per endpoint, since DHIS2 uses a different envelope for each: /api/dataValueSets -> /dataValues (unpaged); metadata lists like /api/organisationUnits or /api/dataElements -> the camelCase plural name, with paginationType page + pageSize; /api/tracker/trackedEntities -> the plural type name on 2.41+ but "instances" on 2.38-2.40. Raw /api/analytics is NOT supported: it returns headers[] plus positional rows[][] rather than record objects - use /api/analytics/dataValueSet.json (responsePath /dataValues) instead. Writing back to DHIS2 is not supported yet: the generic REST sink cannot chunk an import and discards the import summary, and DHIS2 returns HTTP 200 even when every record is rejected.'),
                    src('sap.rfc', 'SAP RFC (SOAP)', 'available', 'Call an RFC-enabled function module / BAPI exposed as a SOAP web service (SOAMANAGER, or the generic /sap/bc/soap/rfc endpoint). Native HTTP + XML, no proprietary SAP NW RFC SDK. Set url to the service endpoint, body to the SOAP envelope, responsePath to the element-name walk to the result table, and add Content-Type / SOAPAction via headers. Binary RFC over the closed SDK is not supported.'),
                ],
            },
            {
                id: 'src.nosql',
                label: 'NoSQL & Search',
                components: [
                    src('mongodb', 'MongoDB', 'available', 'Read documents from a MongoDB collection via the official Rust driver (find with optional filter / projection / limit). Auth via mongodb:// connection string.'),
                    src('cassandra', 'Cassandra', 'available', 'Read CQL via the scylla driver (works with both Cassandra and ScyllaDB).'),
                    src('scylla', 'ScyllaDB', 'available', 'Read CQL via the scylla driver. Same wire as src.cassandra.'),
                    src('redis', 'Redis', 'available', 'SCAN keys matching a pattern (default *) and GET each value via the sync `redis` Rust client. Emits {key, value} rows. limit caps the walk so a million-key DB doesn\'t spin forever.'),
                    src('dynamodb', 'DynamoDB', 'available', 'Scan a DynamoDB table via direct HTTP + AWS SigV4 signing (no aws-sdk-rust dep). Auto-unwraps the typed-attribute response shape ({S: x}, {N: 5}, {BOOL: t}, {L: [...]}, {M: {...}}) into plain JSON. Pagination follows LastEvaluatedKey. Props: region, accessKeyId, secretAccessKey, sessionToken (optional, for STS), tableName, limitPerPage (default 1000), maxPages (safety net, default 100).'),
                    src('neo4j', 'Neo4j', 'available', 'Run Cypher against Neo4j over the HTTP Query API (/db/{database}/query/v2) - works with a self-hosted server and with Aura, and needs no Bolt driver. Basic auth; optional Cypher $parameters. Node and relationship values keep their properties as structs.'),
                    src('elastic', 'Elasticsearch', 'available', 'Read docs from an Elasticsearch index via the _search API. from+size pagination (up to 10000 rows by default); ApiKey auth.'),
                    src('opensearch', 'OpenSearch', 'available', 'Read docs from an OpenSearch index via the _search API. Same wire as Elasticsearch; same ApiKey auth.'),
                    src('couchdb', 'CouchDB', 'available', 'Read CouchDB documents via the _all_docs endpoint (include_docs=true). Rides src.rest - Basic auth, responsePath /rows, cursor pagination via `next_key` if configured.'),
                ],
            },
            {
                id: 'src.misc',
                label: 'Other',
                components: [
                    src('ftp', 'FTP', 'available', 'List + download files from an FTP server via the pure-Rust suppaftp client. Glob pattern filter (`*`, `?`); each file becomes one row {filename, size, modified, content_b64}. Use DuckDB `from_base64(content_b64)` downstream for the raw bytes. SFTP is a separate protocol and a separate component.'),
                    src('http', 'HTTP', 'available', 'Read CSV / Parquet / JSON from any HTTP(S) URL via httpfs'),
                    src('email', 'Email (IMAP)', 'available', 'Fetch the N most recent messages from an IMAP mailbox. TLS via rustls (default port 993). Basic auth (user/password). Each message becomes a row {uid, from, to, subject, date, body_text}. OAuth (gmail / o365) is on the roadmap.'),
                    src('git', 'Git Repository', 'available', 'Read commit log or file tree from a local git working copy. Shells out to the system `git` CLI - no extra Rust dep. mode=log emits {hash, short_hash, author_name, author_email, date, subject}; mode=files emits {mode, type, hash, size, path}.'),
                    src('clipboard', 'Clipboard', 'available', 'Read the system clipboard via pure-Rust arboard. If the text parses as JSON-array-of-objects each element becomes a row; otherwise a single {text, length} row is emitted. Fails clearly on headless Linux (no display server) - desktop-only by design.'),
                ],
            },
            {
                id: 'src.vector',
                label: 'Vector / AI Databases',
                components: [
                    src('pgvector', 'pgvector (Postgres)', 'available', 'Read embeddings + metadata via DuckDB postgres ATTACH (server must have CREATE EXTENSION vector)'),
                    src('pinecone', 'Pinecone', 'preview', 'Fetch or similarity-search vectors (Pinecone has no list-all-vectors endpoint; the proper shape is a query node, on the roadmap)'),
                    src('qdrant', 'Qdrant', 'available', 'Scroll all points in a Qdrant collection via /collections/{id}/points/scroll. Cursor pagination on `result.next_page_offset`; emits {id, ...payload[, vector]} rows. apiKey via api-key header.'),
                    src('weaviate', 'Weaviate', 'available', 'List Weaviate objects via GET /v1/objects?class=&after=. Cursor pagination on the last object\'s id; emits {id, ...properties[, vector]} rows. apiKey via Bearer.'),
                    src('chroma', 'Chroma', 'preview'),
                    src('milvus', 'Milvus', 'available', 'Query Milvus via POST /v1/vector/query. Offset pagination on `offset` + `limit`; emits each `data[]` element as a row. Provide a filter expression (default `id > 0`) and optional outputFields. apiKey via Bearer.'),
                    src('pixeltable', 'Pixeltable', 'available', 'Read a Pixeltable table (#223), the multimodal AI data store. Exchanges through Parquet: Pixeltable exports the table (or a filtered/limited subset) and Duckle ingests it with read_parquet, so no rows cross one at a time. Supports versioned reads via table:N. Needs a Python with pixeltable installed; the desktop app provisions one on first use.'),
                    src('lancedb', 'LanceDB', 'available', 'Read a Lance table (local dir, LanceDB Cloud db://, or s3:// / gs:// / az:// object store) via the bundled duckle-lance sidecar.'),
                ],
            },
        ],
    },
    {
        id: 'transforms',
        label: 'Transforms',
        icon: '∼',
        accent: '#3d8bff',
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
                    xf('uuid', 'UUID', 'available', 'Add a fresh UUID v4 column per row - the standard surrogate row id'),
                    xf('surrogatekey', 'Surrogate Key', 'available', 'Add a warehouse dimension key derived from the business/natural key columns: hash mode (md5 of the key, stable across runs so the same business key always maps to the same surrogate) or sequence mode (1..N integer ordered by the key). Unlike UUID (random per row), this is deterministic.'),
                    xf('compare', 'Compare Columns', 'available', 'Boolean column from comparing two row columns (=, !=, <, <=, >, >=)'),
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
                    xf('rank.filter', 'Top N per Group', 'available', 'Keep the top N rows per group, ordered by a column (row_number window + filter)'),
                    xf('fill_forward', 'Forward Fill', 'available', 'Replace NULL values with the most recent non-null value within an ordered window (time-series gap fill)'),
                    xf('fill_backward', 'Backward Fill', 'available', 'Replace NULL values with the next non-null value within an ordered window (pandas-style bfill / fill up)'),
                    xf('fill_constant', 'Constant Fill', 'available', 'Replace NULL values with a literal value (numbers pass through unquoted; everything else is treated as a string)'),
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
                    xf('cumulative', 'Cumulative', 'available', 'Running sum / avg / count / min / max over an ordered window'),
                    xf('count', 'Count Rows', 'available'),
                    xf('approx.quantile', 'Approx Quantile', 'available', 'Approximate quantile (median, p95, p99) via t-digest - fixed memory regardless of cardinality'),
                ],
            },
            {
                id: 'xf.join',
                label: 'Join',
                components: [
                    xf('join', 'Join', 'available', 'Inner / left / right / full outer join, chosen by the Type dropdown'),
                    xf('join.cross', 'Cross Join', 'available'),
                    xf('join.spatial', 'Spatial Join', 'available', 'Two-input join whose predicate is ST_Intersects / Contains / Within / Touches / Crosses / Overlaps / Equals'),
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
                    xf('tumble', 'Tumbling Window', 'available', 'Event-time tumbling windows that survive across runs. Rows are held until their window CLOSES, decided by a watermark (the greatest event time seen so far) rather than the wall clock - so replaying old data produces the windows that data belongs to instead of closing them all at once. Adds window_start / window_end. allowedLateness holds a window open past its end for out-of-order arrivals; anything later than that is dropped and counted, rather than re-emitted as a second partial copy of a window already delivered. Open windows and the watermark ride the deferred flush, so a failed batch keeps them.'),
                    xf('artifact.copy', 'Copy Artifact', 'available', 'Land the BYTES of the artifacts named upstream somewhere durable, and emit a row per landed copy. Reads a uri column (whatever src.changed, src.artifact or a query produced) and copies from https://, s3://, sftp:// or a local path to an s3:// prefix or a local directory. Streamed and hashed in ONE pass: memory is bounded by the part size, not by the object, so a 40GB file does not become 40GB of RSS, and the sha256 is of the bytes that actually transferred. Naming: keep the source name, preserve its path under the prefix, or content-address it by hash. ifExists skip leaves an immutable raw zone alone rather than re-uploading. Emits uri / source_uri / name / media_type / size_bytes / sha256 / copied.'),
                    xf('archive.extract', 'Extract Archive', 'available', 'Turn one archive artifact into one artifact per member. Reads a uri column of archives - ZIP, TAR, TAR.GZ or GZIP - and lands each member at an s3:// prefix or a local directory, emitting archive_uri / member_name / member_index / uri / media_type / compressed_size / size_bytes / sha256 so each member flows into whichever parser suits it. Generic on purpose: a ZIP of CSVs, a TAR of JSON and a GZIP of NDJSON all land the same way. TAR and GZIP are streamed straight from the source; a ZIP is spooled one archive at a time because its central directory is at the END of the file. Include and exclude globs pick members. An archive is a compression format, so an expansion limit refuses one that would fill the volume rather than discovering it from a disk-full error, and a member path can never escape the destination.'),
                    xf('sessionize', 'Sessionize', 'available', 'Assign a session id to event rows by inactivity gap (clickstream / analytics prep): a new session starts when the time gap from the previous event in the partition exceeds the threshold. Emits session_id (per-partition running integer) and optionally session_seq (event index within the session).'),
                ],
            },
            {
                id: 'xf.strings',
                label: 'Strings',
                components: [
                    xf('regex', 'Regex Replace', 'available'),
                    xf('regex.extract', 'Regex Extract', 'available', 'Extract a capture group from a column via regexp_extract'),
                    xf('regex.match', 'Regex Match', 'available', 'Boolean: does the regex match the column? (regexp_matches)'),
                    xf('url.parse', 'URL Parse', 'available', 'Extract scheme / host / port / path / query / fragment from a URL column'),
                    xf('text.similarity', 'Text Similarity', 'available', 'Pairwise string similarity between two columns - levenshtein / damerau / jaccard / jaro-winkler'),
                    xf('text.base64', 'Base64', 'available', 'Encode a column to base64 text, or decode base64 back to bytes'),
                    xf('text.padding', 'Pad String', 'available', 'Left or right pad to a fixed length (zero-pad IDs, right-pad for fixed-width output)'),
                    xf('text.match', 'Text Match', 'available', 'Boolean: does the string contain / start with / end with a substring (DuckDB contains / starts_with / ends_with)'),
                    xf('text.reverse', 'Reverse', 'available', 'Reverse the characters of a string column'),
                    xf('text.repeat', 'Repeat', 'available', 'Repeat a string column N times'),
                    xf('text.replace', 'Replace (literal)', 'available', 'Literal substring replace (no regex metacharacters)'),
                    xf('text.slug', 'Slug', 'available', 'Generate a URL-safe slug: lowercase + hyphens, no punctuation'),
                    xf('text.strip_html', 'Strip HTML', 'available', 'Remove HTML tags from a column (regex-based, keeps the text content)'),
                    xf('text.tocolumns', 'Text to Columns', 'available', 'Split a delimited column into separate named columns (split_part), e.g. "31.21 30.24" into latitude and longitude'),
                    xf('split', 'Split', 'available', 'Split a column into a LIST in one column - use Text to Columns for separate columns'),
                    xf('concat', 'Concat', 'available'),
                    xf('trim', 'Trim', 'available'),
                    xf('case', 'Case Change', 'available'),
                    xf('length', 'Length', 'available'),
                    xf('substring', 'Substring', 'available'),
                    xf('format', 'Format String', 'available'),
                    xf('hash', 'Hash', 'available', 'Hash a column (md5 / sha1 / sha256) for anonymization or deterministic IDs'),
                    xf('ip.parse', 'IP Parse', 'available', 'Extract host / family / netmask / broadcast from IP or CIDR text via the inet extension'),
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
                    xf('dt.bin', 'Time Bin', 'available', 'Round timestamps down to fixed-interval buckets (e.g. 5 minutes, 1 hour) for time-series grouping'),
                    xf('dt.now', 'Current Timestamp', 'available', 'Add a column with the pipeline run time - the standard loaded_at / processed_at stamp'),
                    xf('dt.epoch', 'Epoch Convert', 'available', 'Convert a TIMESTAMP to Unix epoch seconds, or epoch seconds back to TIMESTAMP'),
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
                    xf('num.bucketize', 'Bucketize', 'available', 'Bin a numeric column into N equal-width buckets between low and high (width_bucket)'),
                    xf('num.zscore', 'Z-Score', 'available', 'Per-row standardized value: (value - mean) / stddev across the whole input'),
                    xf('num.clamp', 'Clamp', 'available', 'Clip values to a [low, high] range - cap outliers before stats'),
                    xf('num.sign', 'Sign', 'available', 'Sign of a number: -1, 0, or +1'),
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
                    xf('json.array_agg', 'Array Aggregate', 'available', 'Collapse rows into a JSON array per group (json_group_array)'),
                    xf('jq', 'jq Filter', 'available', 'Transform a JSON column with a jq program (in-process jaq, no external jq)'),
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
                    xf('arr.length', 'Array Length', 'available', 'Scalar length of a list / array column'),
                    xf('zip', 'Zip Arrays to Table', 'available', 'Zip a headings list and a list of row-arrays (e.g. {headings:[...], rows:[[...]]}) into one row per record with a real column per heading'),
                ],
            },
            {
                id: 'xf.cdc',
                label: 'CDC / SCD',
                components: [
                    xf('incremental', 'Incremental Load', 'available', 'Pass only rows whose watermark column (e.g. updated_at, id) is past the last successful run. The new high-water mark is saved to workspace state and advances only when the whole run succeeds - so reruns never skip rows that were not delivered.'),
                    xf('cdc.diff', 'Diff Detect', 'available', 'Tag inserted/updated/deleted rows vs a previous snapshot'),
                    xf('diffsummary', 'Diff Summary', 'available', 'Reduce a change feed (a change_type column, e.g. from DuckLake Data Diff) to a single summary row: added / removed / updated / total_changes counts plus a ready-made summary text. Feed it into LLM Transform for an AI narrative, or into a validator to assert expected counts in CI.'),
                    xf('cdc.scd1', 'SCD Type 1', 'available', 'Resolved current state: cur + prev rows whose key is not in cur'),
                    xf('cdc.scd2', 'SCD Type 2', 'available', 'Maintain versioned history: close changed rows, insert new versions'),
                    xf('cdc.scd3', 'SCD Type 3', 'available', 'Keep the PREVIOUS value of each tracked attribute in a sibling previous_<col> column. Main input is the current rows; connect the prior snapshot to the previous (lookup) port. Per tracked column, outputs current + previous_<col> joined on the key (NULL for new keys). Optional effective-date stamp.'),
                    xf('cdc.upsert', 'Merge / Upsert', 'available', 'Emit the upsert payload: new + changed rows from cur'),
                    xf('row_hash', 'Row Hash (fingerprint)', 'available', 'Hash N columns into one fingerprint column. md5 / sha1 / sha256. Stable across runs - feed downstream diff / dedup / change detection'),
                    xf('audit', 'Audit Stamp', 'available', 'Append _loaded_at / _loaded_date / _source / _batch_id columns to every row. Standard warehouse provenance pattern'),
                ],
            },
            {
                id: 'xf.ai',
                label: 'AI',
                components: [
                    xf('ai.embed', 'Embeddings', 'available', 'Per-row embedding via any OpenAI-compatible /v1/embeddings endpoint. Props: inputColumn (default `text`), outputColumn (default `embedding`), model (default `text-embedding-3-small`), apiKey (required, sent as Bearer), baseUrl (default `https://api.openai.com` - point at Cohere, Voyage, llama.cpp embed server, etc), batchSize (default 100).'),
                    xf('ai.llm', 'LLM Transform', 'available', 'Per-row LLM completion via any OpenAI-compatible /v1/chat/completions endpoint. Props: promptTemplate with `{column}` substitution (or inputColumn for passthrough), outputColumn (default `completion`), model (default `gpt-4o-mini`), apiKey (required), baseUrl, systemPrompt, temperature. One HTTP call per row - use xf.rows.head to sample before unleashing on big tables.'),
                    xf('ai.chunk', 'Text Chunker', 'available', 'Split long text into chunks for RAG / embedding pipelines. No API call - pure local char-window splitting with overlap. Props: inputColumn (default `text`), outputColumn (default `chunk`), chunkSize (default 1000), chunkOverlap (default 100), mode (`explode` = one row per chunk with chunk_index/chunk_count, `array` = chunks as a column).'),
                    xf('ai.pii', 'PII Redact', 'available', 'Regex-based PII redaction (email, phone, SSN, credit card). No API call. Props: inputColumn (default `text`), outputColumn (defaults to input - overwrites in place), types (comma-list subset; empty = all). LLM-backed redaction is a follow-up.'),
                    xf('ai.classify', 'Classify', 'available', 'Per-row LLM-backed classification. Props: inputColumn (default `text`), outputColumn (default `category`), categories (required, comma-separated list), model (default `gpt-4o-mini`), apiKey, baseUrl. The model is prompted to pick exactly one category; anything outside the list normalizes to `UNKNOWN`. One HTTP call per row.'),
                    xf('ai.dedupe', 'Semantic Dedupe', 'available', 'Drop near-duplicate rows by cosine similarity over a pre-computed embedding column (typically from xf.ai.embed upstream). Props: embeddingColumn (default `embedding`), threshold (default 0.95). No API call; pure local math. O(N^2) - chain after xf.rows.head if your dataset is huge.'),
                    xf('ai.vector_search', 'Vector Similarity Search', 'available', 'Rank rows by similarity to a query vector via DuckDB vss'),
                    xf('ai.text_search', 'Full-Text Search', 'available', 'BM25 keyword search over text columns via DuckDB fts'),
                ],
            },
            {
                id: 'xf.geo',
                label: 'Geospatial',
                components: [
                    xf('geo.distance', 'Spatial Distance', 'available', 'Distance from each row to a target geometry; auto-picks planar or spheroid from the CRS'),
                    xf('geo.length', 'Spatial Length', 'available', 'Length of each line; auto-picks planar or spheroid metres from the CRS'),
                    xf('geo.perimeter', 'Spatial Perimeter', 'available', 'Perimeter of each polygon; auto-picks planar or spheroid metres from the CRS'),
                    xf('geo.area', 'Spatial Area', 'available', 'Area of each polygon; auto-picks planar or spheroid metres from the CRS'),
                    xf('geo.buffer', 'Spatial Buffer', 'available', 'A buffered geometry around each row (ST_Buffer)'),
                    xf('geo.intersects', 'Spatial Intersects', 'available', 'Boolean: does each row overlap a target geometry? (ST_Intersects)'),
                    xf('geo.flip', 'Flip Coordinates', 'available', 'Swap X/Y of every vertex to fix lat,lon vs lon,lat order (ST_FlipCoordinates)'),
                    xf('geo.setcrs', 'Define Projection', 'available', 'Assign a CRS to geometry with missing/unknown CRS, without moving the coordinates (ST_SetCRS)'),
                    xf('geo.reproject', 'Reproject Geometry', 'available', 'Reproject a geometry column from one CRS to another (ST_Transform)'),
                    xf('geo.create', 'Create Geometry', 'available', 'Build a geometry column from X/Y coordinates, WKT, or WKB (ST_Point / ST_GeomFromText / ST_GeomFromWKB)'),
                    xf('geo.clip', 'Clip Geometry', 'available', 'Two-input overlay (#217): keeps every attribute of the input layer on the main input and replaces its geometry with the part inside the clip layer on the second input. The clip layer is dissolved with ST_Union_Agg first, so a feature spanning several clip polygons yields one row rather than one per polygon. Features that do not intersect are dropped. Both layers must share a CRS or the run fails naming each one.'),
                    xf('geo.erase', 'Erase Geometry', 'available', 'Two-input overlay (#218): keeps every attribute of the input layer on the main input and subtracts the erase layer on the second input (ST_Difference). The erase layer is dissolved with ST_Union_Agg first, since differencing each feature in turn would only remove the last. Features left with no geometry are dropped. Both layers must share a CRS or the run fails naming each one.'),
                ],
            },
            {
                id: 'xf.debug',
                label: 'Debug',
                components: [
                    xf('log', 'Log Rows', 'available', 'Pass rows through and print them to Output'),
                    xf('assert', 'Assert', 'available', 'Hard-fail the pipeline if any row violates a SQL predicate (defensive ETL check)'),
                ],
            },
        ],
    },
    {
        id: 'sinks',
        label: 'Sinks',
        icon: '⬆',
        accent: '#ff6900',
        groups: [
            {
                id: 'snk.files',
                label: 'Files',
                components: [
                    snk('csv', 'CSV', 'available'),
                    snk('tsv', 'TSV', 'available', 'Write tab-separated files'),
                    snk('json', 'JSON', 'available'),
                    snk('jsonl', 'JSONL / NDJSON', 'available'),
                    snk('xml', 'XML', 'available', 'Write rows as XML via `quick-xml`. Default shape: `<root><row><col>val</col>...</row>...</root>`. rootElement / rowElement override the wrapper names. Complex (object/array) cell values are JSON-encoded inside CDATA so the file round-trips back through src.xml losslessly.'),
                    snk('excel', 'Excel (XLSX)', 'available', 'Write .xlsx via the DuckDB excel extension'),
                    snk('parquet', 'Parquet', 'available'),
                    snk('vortex', 'Vortex', 'available', 'Write rows as a Vortex columnar file (.vortex) via the bundled duckle-lance sidecar. Next-gen columnar format with fast random access; the engine bridges the upstream rows through Parquet into Vortex.'),
                    snk('avro', 'Avro', 'available', 'Write rows as an Apache Avro container file via the pure-Rust `apache-avro` crate. Schema is inferred from the first row\'s column types (long / double / string / boolean) - or supply a JSON Avro schema via the schemaJson field to override. recordName names the inferred record (default `Row`).'),
                    snk('qvd', 'Qlik QVD', 'available', 'Write rows as a Qlik QVD file (.qvd) via a clean-room pure-Rust encoder (no Qlik runtime). Builds the per-column symbol tables + bit-stuffed index; values are typed per cell (int / double / string), nulls preserved. Round-trips with the QVD source and loads in QlikView / Qlik Sense.'),
                    snk('orc', 'ORC', 'planned'),
                    snk('model', 'Model Card', 'available', 'Register a trained model. The card IS the upstream row - the artifact URI your training script wrote, plus whatever metrics, framework and hashes it recorded - written to <path>/<name>/<version>.json with a latest.json pointer beside it. It needs exactly one row and a version column. The write happens only if the whole run succeeds, so a training pipeline that fails later never registers a model, and a failed retrain never moves the pointer off the model that still works.'),
                    snk('yaml', 'YAML', 'available', 'Write the upstream rows as a top-level YAML array (`- key: value` per row).'),
                    snk('toml', 'TOML', 'available', 'Write the upstream rows as TOML. TOML disallows a top-level array so the engine wraps under a `rows` key: `[[rows]]` per row.'),
                    snk('spatial', 'Geospatial (GeoJSON / GeoPackage / ...)', 'available', 'Write geospatial files via the spatial extension'),
                    snk('ftp', 'File Transfer', 'available', 'Upload pipeline output over FTP / FTPS / SFTP'),
                    snk('huggingface', 'Hugging Face dataset', 'available', 'Push the pipeline output to a Hugging Face Hub dataset repo. The engine materializes a Parquet and commits it over the Hub API (create-repo -> preupload -> git-LFS -> commit). Needs a write-scoped token; the repo is created if it does not exist.'),
                ],
            },
            {
                id: 'snk.lakehouse',
                label: 'Lakehouse table formats',
                components: [
                    snk('iceberg', 'Apache Iceberg', 'available', 'Write a full Iceberg table (data/ + metadata/) via DuckDB v1.5'),
                    snk('ducklake', 'DuckLake', 'available', 'Write a table into a DuckLake catalog'),
                ],
            },
            {
                id: 'snk.databases',
                label: 'Databases',
                components: [
                    snk('postgres', 'PostgreSQL', 'available', 'Write to PostgreSQL via the DuckDB postgres extension'),
                    snk('cockroach', 'CockroachDB', 'available', 'Write to CockroachDB via the DuckDB postgres extension (Cockroach speaks the Postgres wire protocol)'),
                    snk('mysql', 'MySQL', 'available', 'Write to MySQL via the DuckDB mysql extension'),
                    snk('mariadb', 'MariaDB', 'available', 'Write to MariaDB via the DuckDB mysql extension (MariaDB speaks the MySQL wire protocol)'),
                    snk('sqlserver', 'SQL Server', 'available', 'INSERT to SQL Server via TDS (multi-row VALUES batched at 1000 rows, the SQL Server cap).'),
                    snk('oracle', 'Oracle', 'available', 'INSERT to Oracle via the official `oracle` Rust crate. Built into the shipped binary - users need Oracle Instant Client on the library path at runtime. Multi-row INSERT ALL ... SELECT 1 FROM dual idiom batched at 1000 rows.'),
                    snk('teradata', 'Teradata', 'available', 'Write to Teradata through its free ODBC driver. Install the Teradata ODBC driver, then connect with friendly fields, a DSN, or a full ODBC connection string. Append creates the table if missing then appends; Overwrite clears it first. No upsert.'),
                    snk('db2', 'IBM DB2', 'available', 'Write to IBM DB2 through the IBM Data Server ODBC driver. Creates the table if missing from the upstream column types; Append adds rows, Overwrite clears it first. Booleans land in SMALLINT as 1/0, which DB2 for z/OS also accepts. No upsert.'),
                    snk('sqlite', 'SQLite', 'available', 'Write a table into a SQLite file'),
                    snk('turso', 'Turso / libSQL', 'available', 'INSERT rows into a Turso (libSQL) database over the HTTP pipeline API. Creates the table if missing from the upstream column types; Append adds rows, Overwrite clears it first. Values go up as bound parameters, batched (default 500).'),
                    snk('duckdb', 'DuckDB', 'available', 'Write a table into a DuckDB file'),
                    snk('clickhouse', 'ClickHouse', 'available', 'INSERT to ClickHouse via the HTTP interface (FORMAT JSONEachRow). Batched at 10k rows by default.'),
                    snk('execsource', 'Execute in Source', 'available', 'In-database processing: run a CREATE TABLE AS query on the source server itself (Postgres / MySQL) via postgres_execute / mysql_execute. The transform executes in the database and the result lands there, with no round-trip through DuckDB. Self-contained: no input needed.'),
                    snk('jdbc', 'Generic JDBC', 'planned'),
                ],
            },
            {
                id: 'snk.warehouses',
                label: 'Cloud Warehouses',
                components: [
                    snk('motherduck', 'MotherDuck', 'available', 'Write a table into MotherDuck via ATTACH md:'),
                    snk('quack', 'DuckDB Quack', 'available', 'Write a table to a remote DuckDB instance over the Quack protocol (HTTP on port 9494). Supports append / overwrite / truncate / upsert modes via the standard relational sink path.'),
                    snk('gizmosql', 'GizmoSQL', 'available', 'Write rows to a table on a GizmoSQL (Arrow Flight SQL) server via CREATE + batched INSERT over the clean-room pure-Rust Flight SQL client. Append or overwrite; TLS optional.'),
                    snk('snowflake', 'Snowflake', 'available', 'INSERT to a Snowflake table via the SQL API (/api/v2/statements) with PAT (Personal Access Token) bearer auth. Multi-row INSERTs batched at 1000 rows by default.'),
                    snk('bigquery', 'BigQuery', 'available', 'Write tables to BigQuery via the duckdb-bigquery community extension'),
                    snk('redshift', 'Redshift', 'available', 'Write Redshift via the postgres ATTACH path (Postgres wire on port 5439); overwrite / append / truncate / upsert all supported via the existing PG sink modes'),
                    snk('databricks', 'Databricks SQL', 'available', 'INSERT to a Databricks table via the Statement Execution API with PAT Bearer auth. Multi-row INSERTs batched at 1000 rows; sync wait up to 50s.'),
                    snk('synapse', 'Azure Synapse', 'available', 'Azure Synapse rides the SQL Server TDS wire - same connection form as snk.sqlserver.'),
                ],
            },
            {
                id: 'snk.saas',
                label: 'SaaS Connectors',
                components: [
                    snk('salesforce', 'Salesforce', 'available', 'Write rows into a Salesforce object via the REST sObject Collections API (<=200 records/request). insert / update / upsert (by external Id) / delete; auth is a Bearer token or OAuth 2.0 client-credentials (a fresh token minted per run from a connected app). For migration-scale loads use Salesforce Bulk.'),
                    snk('salesforce.bulk', 'Salesforce Bulk', 'available', 'Write rows into a Salesforce object via Bulk API 2.0 - the migration-scale path. DuckDB streams the upstream to CSV on disk and each <=90MB part runs as an async job (insert / update / upsert / delete / hardDelete). Same Bearer / OAuth client-credentials auth as Salesforce. Result sets are written to an optional Results directory.'),
                ],
            },
            {
                id: 'snk.storage',
                label: 'Object Storage',
                components: [
                    snk('s3', 'Amazon S3', 'available', 'Write via DuckDB httpfs'),
                    snk('gcs', 'Google Cloud Storage', 'available', 'Write via DuckDB httpfs'),
                    snk('azureblob', 'Azure Blob Storage', 'available', 'Write via the azure extension'),
                    snk('minio', 'MinIO', 'available', 'Write via S3-compatible endpoint'),
                    snk('r2', 'Cloudflare R2', 'available', 'Write via S3-compatible endpoint'),
                    snk('b2', 'Backblaze B2', 'available', 'Write via S3-compatible endpoint'),
                ],
            },
            {
                id: 'snk.streaming',
                label: 'Streaming',
                components: [
                    snk('kafka', 'Apache Kafka', 'available', 'Produce one Kafka record per upstream row via the pure-Rust `rskafka` driver. Record key = optional keyColumn value; record value = JSON-stringified row. Records go to a single partition (partitionId, default 0); pipelined batching (default 500 records per produce call). Every write is acknowledged by the full in-sync replica set (acks=all) - the driver does not offer a weaker setting.'),
                    snk('redpanda', 'Redpanda', 'available', 'Same wire protocol as Kafka - rides the rskafka driver. Use snk.kafka semantics.'),
                    snk('pulsar', 'Apache Pulsar', 'planned'),
                    snk('nats', 'NATS JetStream', 'available', 'Publish each upstream row as one NATS message via the pure-Rust `async-nats` driver. Payload = JSON-stringified row. Optional subjectSuffixColumn appends a per-row suffix (subject.value) for routed multi-tenant publishing.'),
                    snk('rabbit', 'RabbitMQ', 'available', 'Publish each upstream row as one persistent-delivery-mode AMQP 0.9.1 message via the pure-Rust `lapin` driver. Configurable exchange + routingKey; empty exchange = default direct exchange (route to queue named by routingKey).'),
                    snk('pubsub', 'GCP Pub/Sub', 'available', 'Publish messages via the Pub/Sub REST API (POST /v1/projects/{p}/topics/{t}:publish). Each upstream row -> one base64-encoded message. Auth via OAuth2 Bearer access token. Batched at 100 messages per request (Pub/Sub max).'),
                    snk('kinesis', 'AWS Kinesis', 'planned'),
                    snk('websocket', 'WebSocket', 'available', 'Connect to a ws:// or wss:// URL and send each upstream row as a text frame - the whole row as JSON, or one column when messageColumn is set - then close. For pushing processed results to real-time dashboards or WebSocket APIs.'),
                ],
            },
            {
                id: 'snk.apis',
                label: 'APIs',
                components: [
                    snk('rest', 'REST', 'available', 'HTTP POST one batched request containing the result as a JSON array (configurable method, headers, body shape)'),
                    snk('webhook', 'Webhook', 'available', 'HTTP POST one request per row, body = row JSON (configurable method + headers)'),
                    snk('dhis2', 'DHIS2', 'available', 'Import rows into DHIS2. Set url to https://<host>/api/dataValueSets (importType aggregate) or https://<host>/api/tracker (importType tracker + trackerResource trackedEntities/events/enrollments/relationships). Rows are chunked (chunkSize, default 1000) and wrapped in the collection key DHIS2 expects. importStrategy defaults to CREATE_AND_UPDATE, which IS the DHIS2 upsert; dryRun validates without committing. The import summary is parsed, not discarded: conflicts, error reports and a non-zero ignored count fail the run by default (failOnConflict), because DHIS2 answers HTTP 200 even when it rejects every record. Sent synchronously (async=false) so the outcome is known per chunk rather than left in a job queue.'),
                    snk('graphql', 'GraphQL Mutation', 'available', 'POST a GraphQL mutation per upstream row. The mutation body can reference row fields via ${field} substitution.'),
                    snk('email', 'Email (SMTP)', 'available', 'Per-row SMTP send via pure-Rust `lettre` + rustls TLS. Props: host (required), port (default 587), user/password (optional - skip for relay-only servers), fromAddress (required), toColumn (default `to`), subjectColumn (default `subject`), bodyColumn (default `body`). Plain text only for v1; HTML / attachments are follow-ups.'),
                ],
            },
            {
                id: 'snk.nosql',
                label: 'NoSQL & Search',
                components: [
                    snk('mongodb', 'MongoDB', 'available', 'Insert documents into a MongoDB collection via the official driver. Bulk insert_many batched at 1000 docs by default; replace mode drops the collection first.'),
                    snk('cassandra', 'Cassandra', 'available', 'INSERT rows into a Cassandra table via the scylla CQL driver (one INSERT per row; CQL has no multi-row VALUES).'),
                    snk('scylla', 'ScyllaDB', 'available', 'Same wire as snk.cassandra - INSERT via the scylla CQL driver.'),
                    snk('redis', 'Redis', 'available', 'SET each row\'s keyColumn -> valueColumn into Redis via the sync `redis` Rust client. Optional ttlSeconds adds an EXPIRE. If valueColumn is empty, the whole row is JSON-stringified as the value. Pipelined in chunks (default 1000).'),
                    snk('neo4j', 'Neo4j', 'available', 'Write rows as Neo4j nodes over the HTTP Query API. Rows ride up as one $rows parameter expanded with UNWIND, so a batch is one round trip. Set mergeKeys to MERGE on those properties (re-running updates the matched nodes) instead of CREATE; or supply your own Cypher that consumes $rows.'),
                    snk('elastic', 'Elasticsearch', 'available', 'Bulk-index docs via the _bulk NDJSON API (configurable host, index, ApiKey auth)'),
                    snk('opensearch', 'OpenSearch', 'available', 'Bulk-index docs via the OpenSearch _bulk NDJSON API (same shape as Elasticsearch)'),
                ],
            },
            {
                id: 'snk.vector',
                label: 'Vector / AI Databases',
                components: [
                    snk('pgvector', 'pgvector (Postgres)', 'available', 'Write embeddings to a Postgres table (server must have CREATE EXTENSION vector)'),
                    snk('pinecone', 'Pinecone', 'available', 'Upsert vectors to a Pinecone index via /vectors/upsert with Api-Key auth'),
                    snk('qdrant', 'Qdrant', 'available', 'Upsert points to a Qdrant collection via PUT /collections/{name}/points'),
                    snk('weaviate', 'Weaviate', 'available', 'Batch upsert objects to a Weaviate cluster via /v1/batch/objects with Bearer auth'),
                    snk('chroma', 'Chroma', 'preview'),
                    snk('milvus', 'Milvus', 'available', 'Insert rows to a Milvus collection via /v1/vector/insert'),
                    snk('pixeltable', 'Pixeltable', 'available', 'Write rows into a Pixeltable table (#223). Duckle COPYs the upstream rows to Parquet and Pixeltable inserts the file directly. Insert appends to an existing table; Create builds one from the incoming rows. Needs a Python with pixeltable installed; the desktop app provisions one on first use.'),
                    snk('lancedb', 'LanceDB', 'available', 'Write rows to a Lance table (create/overwrite or append) via the bundled duckle-lance sidecar.'),
                ],
            },
        ],
    },
    {
        id: 'control',
        label: 'Control Flow',
        icon: '◇',
        accent: '#5b8def',
        groups: [
            {
                id: 'ctl.routing',
                label: 'Routing',
                components: [
                    ctl('replicate', 'Replicate / Tee', 'available', 'Send the same data to multiple downstream outputs'),
                    ctl('switch', 'Switch / Conditional Split', 'available', 'Route rows to case_1..N outputs by condition; first match wins'),
                    ctl('merge', 'Merge Streams', 'available', 'Concatenate multiple input streams (UNION ALL)'),
                    ctl('iterate', 'Iterate', 'available', 'Runs a referenced pipeline N times. Sub-pipeline gets ${ITER_INDEX} (0..N-1) substituted into its props before each call. Side-effect model - sub-pipeline output isn\'t composed into the parent (true block-scope iteration needs the DAG refactor in docs/dag-block-refactor.md).'),
                    ctl('foreach', 'For Each', 'available', 'Runs a referenced pipeline once per upstream row. ${ITER_INDEX} + ${ITER_ITEM_<FIELD>} (uppercased) substituted into the sub-pipeline props. Side-effect model.'),
                ],
            },
            {
                id: 'ctl.timing',
                label: 'Timing',
                components: [
                    ctl('wait', 'Wait / Delay', 'available', 'Sleep for a fixed number of milliseconds before passing rows through (smoke tests, rate-limit a downstream API)'),
                    ctl('schedule', 'Schedule', 'planned'),
                    ctl('throttle', 'Throttle', 'available', 'Insert an inter-stage delay derived from a rows-per-second target (best-effort for batch pipelines, hook is in place for streaming)'),
                ],
            },
            {
                id: 'ctl.pipeline',
                label: 'Pipelines',
                components: [
                    ctl('runpipeline', 'Run Pipeline', 'available', 'Reads + executes another pipeline file inline as a side effect, then passes the upstream view through unchanged. Useful for triggering helper pipelines (refresh dimension tables, kick off cleanup) without composing their output into the parent.'),
                    ctl('trigger', 'Trigger Pipeline', 'available', 'Alias of ctl.runpipeline; same executor branch.'),
                    ctl('runjob', 'Run Job', 'available', 'Calls a child pipeline (job) as a side effect, passing parent context variables that are substituted as ${VAR} into the child before it runs. Chain several Run Job nodes to build a Master Job that orchestrates child jobs in sequence. The child runs in its own temp DB; its output is not composed back into the parent.'),
                    ctl('parallelize', 'Parallelize', 'available', 'Runs the independent downstream branches wired to its outputs concurrently. The upstream input is snapshotted once and each branch reads that snapshot in its own isolated execution, joining when all finish (any branch failure fails the node).'),
                    src('inline', 'Inline Rows', 'available', 'Rows you write here rather than read from anywhere: a control row, an audit stamp, a fixed lookup. Give each column a name and a value; rowCount repeats the row. Every other source names an external system, so this was previously a throwaway file.'),
                    src('artifact', 'Artifacts', 'available', 'One row per FILE described the way a pipeline can reason about it: uri, name, media_type, size_bytes, sha256 and modified_at. For PDFs, images, archives, OCR output and model binaries - an artifact is a reference, not the bytes, so it joins, filters and iterates like any other table. Hashing is off by default because it reads every byte; turn it on when you want reproducibility and can pay for it.'),
                    src('filelist', 'File List', 'available', 'One row per file in a directory - file (full path) and filename - so a pipeline can iterate a folder. Set a glob pattern and optionally recurse. Pair it with ForEach to process every file.'),
                    ctl('runevents', 'Run Events', 'available', 'Rows describing the stages that have already failed in this run: node_id, kind, status, message, category, duration_ms. Wire it into a mail or table sink to report failures. It reports failures the run SURVIVED, so mark the stages that may fail with Continue on failure.'),
                    ctl('file', 'File Operation', 'available', 'One typed filesystem operation: copy, move or delete a file. Staging a file between a landing area and a working area is ordinary batch work; before this the only filesystem-capable component ran a shell command, which cannot serve both platforms from one authored pipeline.'),
                    ctl('anchor', 'Sequence Anchor', 'available', 'Does no work itself. It exists so ordering links have something to attach to: wire a trigger out of it to say what runs after, or into it to say what must finish first. Takes no input and produces no rows, so it never joins the data flow.'),
                    ctl('setvar', 'Set Run Variable', 'available', 'Work out a value while the run is under way and let later steps in the same pipeline ask for it as ${name}: the date on the batch just read, the id just written. Wired to rows the expression is read against them; wired to nothing it stands on its own. The static context cannot carry these, because nothing knows them until the run has started.'),
                    ctl('checkpoint', 'Checkpoint', 'available', 'Pass rows through and also write a parquet snapshot to a path - the durable artifact a future run can read back via src.parquet'),
                ],
            },
            {
                id: 'ctl.errors',
                label: 'Error Handling',
                components: [
                    ctl('try', 'Try / Catch', 'available', 'Installs a fallback pipeline. If any downstream stage in this execution fails, the fallback runs as a side effect before the original error surfaces - useful for notifications, rollbacks, cleanup. Slice of the DAG-block refactor; true continuation-style try/catch needs the multi-week refactor (see docs/dag-block-refactor.md).'),
                    ctl('retry', 'Retry', 'available', 'Per-stage retry already lives in the Advanced tab (Retry attempts + Retry backoff) on every node - no separate component needed. A DAG-scoped retry block (wrap N stages, retry the whole group) still needs the DAG-block refactor; use ctl.try with a recovery fallback for now.'),
                    ctl('deadletter', 'Dead Letter Queue', 'available', 'Terminal sink for rejected rows - parquet or csv at a configurable path; conventionally wired to an upstream node\'s reject port'),
                ],
            },
            {
                id: 'ctl.logging',
                label: 'Logging & Alerts',
                components: [
                    ctl('log', 'Log Message', 'available', 'Emit an info log line, then pass rows through unchanged. Use {rows} in the message for the upstream row count. Lines are written to the run log under the workspace logs/ folder (NDJSON) so Splunk / Dynatrace can ingest them.'),
                    ctl('warn', 'Warn', 'available', 'Emit a warning log line (does not fail the run), then pass rows through. Same {rows} templating and workspace log output as Log Message.'),
                    ctl('die', 'Die / Fail', 'available', 'Stop the pipeline with an error message. Condition controls when it fires: always, only when the input has rows (guard a reject branch), or only when the input is empty (guard missing data).'),
                ],
            },
        ],
    },
    {
        id: 'quality',
        label: 'Data Quality',
        icon: '✓',
        accent: '#ff7a45',
        groups: [
            {
                id: 'qa.validation',
                label: 'Validation',
                components: [
                    qa('baseline', 'Run Baseline', 'available', 'Compare this run against what previous runs looked like. Every row can satisfy the schema and every row-level rule while the dataset is nothing like what normally arrives - 842,114 rows where five million usually come, a null rate that went from 4 percent to 71, a country partition that vanished - and that publishes successfully, which is more dangerous than a crash. Profiles row count, and per column the null count, null rate, distinct count, min, max and mean; compares against the MEDIAN of the last N accepted profiles so one odd day does not move the baseline. Rules take percentage or absolute limits in either direction. groupBy with requireExistingGroups catches a partition disappearing even when the total stays in range. Gate fails the run; report only emits the findings. Deterministic - rolling statistics and explicit thresholds, no model. The new profile is accepted only if the whole run succeeds, so a run that failed downstream never leaves today numbers as the new normal.'),
                    qa('schemavalidate', 'Schema Validate', 'available', 'Reject rows where any expected column is null'),
                    qa('regex', 'Regex Match', 'available', 'Pass rows matching a pattern; rest to reject'),
                    qa('range', 'Range Check', 'available', 'Pass in-range rows; rest to reject'),
                    qa('notnull', 'Not-Null Check', 'available', 'Pass rows with no nulls; rest to reject'),
                    qa('unique', 'Uniqueness Check', 'available', 'Pass first per key; duplicates to reject'),
                    qa('outlier', 'Outlier Detection', 'available', 'Pass in-distribution rows; route statistical outliers (IQR or z-score over the chosen numeric column) to the reject port. NULLs and zero-spread data always pass.'),
                ],
            },
            {
                id: 'qa.profile',
                label: 'Profiling',
                components: [
                    qa('profile', 'Column Profile', 'available', 'Per-column stats: count, nulls, distinct, min/max, quartiles'),
                    qa('profile.adv', 'Column Profile (Advanced)', 'available', 'Rich single-column profile: count, null_count, null_pct, approx distinct, min/max, the fraction of values matching common patterns (email / integer / decimal / date), and the top-N most frequent values with counts. Long-form output: one row per metric (metric, value, count, pct).'),
                    qa('describe', 'Describe', 'available', 'Column names and types of the input'),
                    qa('histogram', 'Histogram', 'available', 'Value frequencies for a column'),
                ],
            },
            {
                id: 'qa.cleanse',
                label: 'Cleansing',
                components: [
                    qa('standardize', 'Standardize', 'available', 'Trim, case-normalize, and collapse whitespace'),
                    qa('mask', 'Mask / Anonymize', 'available', 'Irreversibly mask a column in place for governance/compliance: deterministic salted-hash pseudonym (joinable across datasets), partial mask (show last N), null-out, or a constant. Pure in-engine, no data leaves your machine.'),
                    qa('dedupe', 'Fuzzy Deduplicate', 'available', 'Drop near-duplicate rows by string similarity'),
                    qa('match', 'Record Match', 'available', 'Find matching record pairs by similarity, with a score'),
                    qa('survivor', 'Survivorship (Golden Record)', 'available', 'Collapse duplicate records sharing a key into one golden record, picking each surviving field by rule: most-frequent value, most-recent / oldest (by a date column), or max / min. Applies to every non-key column at once.'),
                    qa('matchgroup', 'Match Grouping (Cluster IDs)', 'available', 'Turn a list of matched record pairs into one stable cluster id per record. Walks the transitive closure of the matches (a~b and b~c put a, b, c in one cluster) and assigns each id the cluster representative (the smallest reachable id). Pairs with Record Match. Output: id, cluster_id.'),
                    qa('expect', 'Expectations', 'available', 'Run a reusable suite of data-quality expectations (not-null, unique, in-set, in-range, regex, non-negative) and emit a scorecard: one row per rule with total, failed, pass_rate, and passed. The native, no-Python answer to declarative data contracts.'),
                    qa('contract', 'Data Contract', 'available', 'Enforce a data contract: the same rule suite as Expectations (not-null, unique, in-set, in-range, regex, non-negative), but as a GATE. Passes every row through unchanged when all rules hold, and fails the run with a clear error naming the violated rule(s) when any rule breaks. Drop it before a sink or scheduled load to block bad data in CI.'),
                    qa('freshness', 'Freshness Check (SLA Gate)', 'available', 'Assert the data is recent enough: computes data age = now - max(timestamp column) and checks it against a maxAge (in minutes / hours / days). Gate mode passes every row through unchanged when the freshest row is within the SLA and fails the run with a clear message when it is not. Report mode emits a one-row scorecard (max_timestamp, age, threshold, is_fresh) for dashboards or CI.'),
                    qa('sample.adv', 'Sample (Reproducible %)', 'available', 'Take a percentage sample of rows. Reservoir (even per-row probability) or Bernoulli (independent per row); set a seed to make the draw reproducible so the same rows are picked every run. All columns are preserved.'),
                    qa('refintegrity', 'Referential Integrity', 'available', 'Check a foreign key against a reference input (connect it to the lookup port): rows whose key exists in the reference pass through, orphan rows (key missing) route to the reject port. Pure semi-join / anti-join, no row fan-out on duplicate reference keys.'),
                    qa('block', 'Candidate Pairs (Blocking)', 'available', 'Cut an entity-resolution job down to the pairs worth comparing. Every fuzzy match compares pairs, and comparing all of them grows with the product of the row counts, so blocking proposes only records that already agree on something cheap and discriminating (same postcode, same surname). One input dedupes within a table; wire the lookup port to link two. Rules are named and can overlap - a pair caught by several is still emitted once. Output: id_a, id_b, blocking_rule, and a_<col>/b_<col> for carried columns, which is exactly what Match Grouping reads by default.'),
                    qa('link', 'Record Linkage', 'available', 'Fuzzy-link records across TWO inputs: the main input against a reference on the lookup port. Cross-compares the chosen key columns by string similarity (Jaro-Winkler or Levenshtein) and emits every candidate pair at or above the threshold as left_key, right_key, score. Unlike Record Match (self-join), this links two separate datasets.'),
                    qa('reconcile', 'Reconcile (Source vs Target)', 'available', 'Two-source reconciliation report for migrations and CDC QA. Main input is the source; connect the target to the lookup port. Joins on your key column(s) and emits one row per metric: source_rows, target_rows, rows_only_in_source, rows_only_in_target, keys_matched, plus per measure a source_sum / target_sum / difference.'),
                    qa('classify', 'Classify / PII Detect', 'available', 'Heuristically classify each column by semantic / PII type - pure regex + statistics, no model. Measures the fraction of values matching known shapes (email, SSN, credit card, IPv4, UUID, URL, phone, date) and tags the best match above a threshold. Emits a report (column, detected_type, match_rate, sample_count, is_pii); pair it with Mask / Anonymize.'),
                    qa('addressclean', 'Address Cleanse', 'planned'),
                ],
            },
            {
                id: 'qa.geometry',
                label: 'Geometry',
                components: [
                    qa('geomvalidate', 'Validate Geometry', 'available', 'Flag invalid geometries with ST_IsValid: add an is_valid column (keep all), or keep only valid / only invalid features.'),
                    qa('geomrepair', 'Repair Geometry', 'available', 'Repair invalid geometries in place with ST_MakeValid: fix all geometries, or only the invalid ones (valid features pass through untouched).'),
                    qa('geomempty', 'Check Empty Geometry', 'available', 'Flag empty geometries with ST_IsEmpty: add an is_empty column (keep all), or keep only empty / only non-empty features.'),
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
                    code('python', 'Python UDF', 'available', 'Transform via a real Python 3 interpreter (full language + installed packages). Define `process(row)` to work a row at a time (a dict in, a dict or None out, JSON both ways), or `transform(table)` to be handed the WHOLE table at once as a pyarrow Table - use that for polars/pandas/PyArrow work, OCR, entity resolution or ML, where a row at a time is the wrong shape. Or `transform_batches(batch)` to be streamed a RecordBatch at a time (65,536 rows), which never holds the whole table in memory - use that when the data is larger than RAM. transform and transform_batches also keep types: through the row path a timestamp arrives as a string. Both need pyarrow in the interpreter; process(row) needs nothing beyond Python. The script can also read INPUT_PATH, the Parquet file the rows arrive in, to scan it with polars, DuckDB or a pyarrow Dataset directly. Needs Python 3 on PATH or DUCKLE_PYTHON_BIN. Code in the `code` prop.'),
                    code('rust', 'Rust UDF', 'planned'),
                    code('javascript', 'JavaScript UDF', 'available', 'Per-row JS transform via the pure-Rust boa interpreter (sandboxed - no fetch / fs / DOM). Define a `transform(row)` function; the engine calls it per row with the row as a JS object and uses the returned object as the output row. Helpers declared at the top of the script are shared across rows within the stage. Script in the `script` prop.'),
                    code('shell', 'Shell Command', 'available', 'Run an arbitrary shell command and emit one row with {stdout, stderr, exit_code, duration_ms}. Defaults to cmd.exe on Windows, /bin/sh on Unix. Optional timeout + workingDir. Cancellation kills the child process.'),
                    code('wasm', 'WebAssembly UDF', 'available', 'Per-row WASM transform via the pure-Rust wasmi interpreter (sandboxed - no fs / net / env access). Supply the module as `wasmB64` (base64) or `path` to a .wasm file. Module must export `memory` and a function `transform(i32, i32) -> i64` packing (out_ptr << 32) | out_len. Defaults: inputColumn=text, outputColumn=result, function=transform.'),
                ],
            },
            {
                id: 'code.dbt',
                label: 'dbt',
                components: [
                    xf('dbt', 'dbt', 'available', "Run dbt against the pipeline's DuckDB database. Either write one inline model right here (reference the upstream node as {{ var('duckle_input') }}), or set `projectDir` to an existing dbt project (folder with dbt_project.yml). The engine generates the dbt-duckdb profiles.yml automatically, so models read upstream node tables and downstream nodes read the models dbt builds. Set `command` (default `run`); optional `outputModel` reads a built model back as the node output. dbt is set up automatically on first launch, so no manual install is needed."),
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
                    src('salesforce', 'Salesforce', 'available', 'Salesforce REST. Rides the generic src.rest path with a Bearer token or OAuth 2.0 client-credentials (a fresh token minted per run from a connected app); users typically point url at https://{instance}.my.salesforce.com/services/data/v60.0/query/?q=SELECT+... and walk responsePath /records.'),
                    src('salesforce.bulk', 'Salesforce Bulk', 'available', 'Salesforce Bulk API 2.0 query source for migration-scale reads: a SOQL query runs as an async query job (query / queryAll incl. deleted+archived), the paged CSV result sets stream to disk via Sforce-Locator, and DuckDB reads them out-of-core - a multi-GB result never lands in memory. Same auth as the Bulk sink (Bearer / client-credentials / saved connection).'),
                    src('hubspot', 'HubSpot', 'available', 'HubSpot REST. Bearer auth via a Private App access token. Cursor pagination on `paging.next.after` (cursorNextPath /paging/next/after, cursorParam `after`). responsePath /results.'),
                    src('pipedrive', 'Pipedrive', 'available', 'Pipedrive REST. URL ?api_token=... or Bearer auth. Cursor pagination on `additional_data.pagination.next_start` (start parameter). responsePath /data.'),
                    src('zendesk', 'Zendesk', 'available', 'Zendesk Support REST. Basic auth (email/token + API token). Cursor pagination via `meta.after_cursor` + `page[after]` param. responsePath /tickets (or whatever resource).'),
                    src('intercom', 'Intercom', 'available', 'Intercom REST. Bearer auth. Cursor pagination via `pages.next.starting_after` + `starting_after` param. responsePath /data.'),
                ],
            },
            {
                id: 'saas.finance',
                label: 'Finance',
                components: [
                    src('stripe', 'Stripe', 'available', 'Stripe REST. Bearer auth with the Secret Key (sk_live_... / sk_test_...). Cursor pagination on `data[-1].id` via `starting_after`. responsePath /data.'),
                    src('quickbooks', 'QuickBooks', 'available', 'QuickBooks Online REST. Bearer OAuth token; users assemble the query URL (Intuit\'s API requires SQL-like queries). responsePath /QueryResponse.'),
                    src('xero', 'Xero', 'available', 'Xero REST. Either paste a Bearer OAuth token, or pick OAuth 2.0 Client Credentials and give the token URL (https://identity.xero.com/connect/token) with HTTP Basic client auth so a fresh token is minted per run - that suits a Xero Custom Connection. Pass Xero-Tenant-Id as a custom header. responsePath defaults to a top-level resource key (e.g. /Invoices).'),
                    src('shopify', 'Shopify', 'available', 'Shopify Admin API. Bearer auth via X-Shopify-Access-Token. Link header pagination supported by recent Admin API endpoints. responsePath depends on resource (e.g. /products).'),
                ],
            },
            {
                id: 'saas.productivity',
                label: 'Productivity',
                components: [
                    src('notion', 'Notion', 'available', 'Notion REST. Bearer integration token + Notion-Version header. Cursor pagination on `next_cursor` (cursorNextPath /next_cursor, cursorParam `start_cursor`). responsePath /results.'),
                    src('airtable', 'Airtable', 'available', 'Airtable REST. Bearer Personal Access Token. Cursor pagination on `offset` (cursorNextPath /offset, cursorParam `offset`). responsePath /records.'),
                    src('asana', 'Asana', 'available', 'Asana REST. Bearer Personal Access Token (https://app.asana.com/0/my-apps). Cursor pagination on `next_page.offset` (cursorNextPath /next_page/offset, cursorParam `offset`). responsePath /data. Base URL https://app.asana.com/api/1.0.'),
                    src('trello', 'Trello', 'available', 'Trello REST. Anonymous-style auth: append `?key={apiKey}&token={token}` to the URL. No body, no pagination (the API returns full result sets by default). Set responsePath empty since responses are top-level arrays. Base URL https://api.trello.com/1.'),
                    src('clickup', 'ClickUp', 'available', 'ClickUp REST. Bearer Personal API token (pk_... from Settings > Apps). Page pagination on `?page=N` (paginationType `page`, pageParam `page`). responsePath /tasks (or whatever resource). Base URL https://api.clickup.com/api/v2.'),
                    src('monday', 'Monday.com', 'available', 'Monday.com GraphQL. Rides src.graphql; auth via Bearer token in Authorization header. POST a GraphQL query as `body`; responsePath /data.<query_name>. Base URL https://api.monday.com/v2.'),
                    src('gsheets', 'Google Sheets', 'planned'),
                    src('excel-online', 'Microsoft Excel Online', 'planned'),
                ],
            },
            {
                id: 'saas.devtools',
                label: 'Dev Tools',
                components: [
                    src('github', 'GitHub', 'available', 'GitHub REST. Bearer Personal Access Token. Link header pagination (paginationType `link`). Accept: application/vnd.github+json header recommended; defaults to https://api.github.com.'),
                    src('gitlab', 'GitLab', 'available', 'GitLab REST. Bearer Personal Access Token. Link header pagination (paginationType `link`). Base URL https://gitlab.com/api/v4 (or self-hosted).'),
                    src('linear', 'Linear', 'available', 'Linear GraphQL. Rides src.graphql; auth via API key in Authorization header. responsePath walks /data.<query>.<edges> or similar.'),
                    src('jira', 'Jira', 'available', 'Jira Cloud REST. Basic auth (email + API token). Offset pagination on `startAt` + `maxResults`. responsePath /issues for /search.'),
                ],
            },
            {
                id: 'saas.marketing',
                label: 'Marketing',
                components: [
                    src('mailchimp', 'Mailchimp', 'available', 'Mailchimp REST. Bearer API key (the key has a region suffix - the URL is https://{region}.api.mailchimp.com/3.0). Offset pagination via `offset` + `count`. responsePath /lists (or /campaigns / etc).'),
                    src('sendgrid', 'SendGrid', 'available', 'SendGrid REST. Bearer API key. Offset pagination via `offset` + `limit`. responsePath /result for /v3/marketing/* endpoints.'),
                    src('segment', 'Segment', 'available', 'Segment Public API. Bearer access token. Cursor pagination via `pagination.next` + `pagination[cursor]` param. responsePath /data.'),
                ],
            },
            {
                id: 'saas.comms',
                label: 'Communication',
                components: [
                    src('slack', 'Slack', 'available', 'Slack Web API. Bearer Bot User OAuth Token (xoxb-...). Cursor pagination via `response_metadata.next_cursor` + `cursor` param. responsePath depends on endpoint (e.g. /messages for conversations.history). Base URL https://slack.com/api.'),
                    src('discord', 'Discord', 'available', 'Discord REST. Bot token in Authorization header (prefix `Bot `). No native pagination on most endpoints; use `?limit=N&before=ID` patterns. responsePath empty (responses are top-level arrays). Base URL https://discord.com/api/v10.'),
                    src('telegram', 'Telegram Bot', 'available', 'Telegram Bot API. Token in URL path (https://api.telegram.org/bot{token}/getUpdates). Offset pagination via `?offset=N`. responsePath /result. No auth header needed - token is in the URL.'),
                    src('twilio', 'Twilio', 'available', 'Twilio REST. Basic auth (Account SID + Auth Token). Page-cursor pagination via `next_page_uri`. responsePath depends on resource (e.g. /messages, /calls). Base URL https://api.twilio.com/2010-04-01/Accounts/{AccountSid}.'),
                ],
            },
        ],
    },
];

// #307: external components live here rather than in PALETTE, because they are
// discovered when a workspace opens rather than compiled in. A tiny store
// instead of prop-threading: Palette takes no props today, and the alternative
// is passing this through every component between it and the app root.
let EXTERNAL: ComponentDef[] = [];
const listeners = new Set<() => void>();

/** Property forms for external components, keyed by id (#307). */
const EXTERNAL_MANIFESTS: Record<string, unknown> = {};

export function setExternalManifest(id: string, manifest: unknown): void {
    EXTERNAL_MANIFESTS[id] = manifest;
}

export function getExternalManifest(id: string): unknown | undefined {
    return EXTERNAL_MANIFESTS[id];
}

export function setExternalComponents(defs: ComponentDef[]): void {
    // Referentially stable when nothing changed, so useSyncExternalStore does
    // not re-render the palette on every workspace poll.
    const same =
        defs.length === EXTERNAL.length &&
        defs.every((d, i) => d.id === EXTERNAL[i].id && d.label === EXTERNAL[i].label);
    if (same) return;
    EXTERNAL = defs;
    listeners.forEach(l => l());
}

export function subscribeExternalComponents(l: () => void): () => void {
    listeners.add(l);
    return () => {
        listeners.delete(l);
    };
}

export function getExternalComponents(): ComponentDef[] {
    return EXTERNAL;
}

/** The palette, plus an "External" category when the workspace has any. */
export function paletteWith(external: ComponentDef[]): Category[] {
    if (external.length === 0) return PALETTE;
    // Its own category rather than mixed into Sources/Transforms: a component
    // Duckle did not write should be visibly not one Duckle wrote.
    const groups: Group[] = [
        { id: 'ext-sources', label: 'Sources', components: external.filter(c => c.kind === 'source') },
        { id: 'ext-transforms', label: 'Transforms', components: external.filter(c => c.kind === 'transform') },
        { id: 'ext-sinks', label: 'Sinks', components: external.filter(c => c.kind === 'sink') },
    ].filter(g => g.components.length > 0);
    return [
        ...PALETTE,
        { id: 'external', label: 'External', icon: 'code', accent: 'slate', groups },
    ];
}

export const ALL_COMPONENTS: ComponentDef[] = PALETTE.flatMap(c => c.groups.flatMap(g => g.components));

export const TOTAL_COMPONENT_COUNT = ALL_COMPONENTS.length;

export const AVAILABLE_COUNT = ALL_COMPONENTS.filter(c => c.availability === 'available').length;

/** True if every char of `term` appears in order within `text` (gap-tolerant
 * subsequence match, e.g. "sf" matches "snowflake"). */
function fuzzySubseq(text: string, term: string): boolean {
    let i = 0;
    for (let j = 0; j < text.length && i < term.length; j++) {
        if (text[j] === term[i]) i++;
    }
    return i === term.length;
}

/**
 * Fuzzy-rank components for the canvas quick-add search. Matches each
 * whitespace-separated term against the label, the id (with the src./snk./xf./
 * etc. prefix both kept and stripped), the summary and the kind; a term may
 * also match as a gap-tolerant subsequence of the label/id. All terms must
 * match (AND). Exact > prefix > substring > subsequence, with available
 * components ranked above preview/planned. Returns the top `limit`.
 */
export function searchComponents(query: string, limit = 14): ComponentDef[] {
    const q = query.trim().toLowerCase();
    if (!q) return [];
    const terms = q.split(/\s+/).filter(Boolean);
    const scored: { c: ComponentDef; score: number }[] = [];
    for (const c of ALL_COMPONENTS) {
        const label = c.label.toLowerCase();
        const id = c.id.toLowerCase();
        const idShort = id.includes('.') ? id.slice(id.indexOf('.') + 1) : id;
        const summary = (c.summary ?? '').toLowerCase();
        let score = 0;
        let matchedAll = true;
        for (const t of terms) {
            let s = 0;
            if (label === t || idShort === t) s = 100;
            else if (label.startsWith(t)) s = 60;
            else if (idShort.startsWith(t)) s = 50;
            else if (label.includes(t)) s = 32;
            else if (idShort.includes(t) || id.includes(t)) s = 22;
            else if (summary.includes(t) || c.kind.includes(t)) s = 12;
            else if (fuzzySubseq(label, t) || fuzzySubseq(idShort, t)) s = 6;
            else { matchedAll = false; break; }
            score += s;
        }
        if (!matchedAll) continue;
        if (c.availability === 'available') score += 8;
        else if (c.availability === 'preview') score += 3;
        scored.push({ c, score });
    }
    scored.sort((a, b) => b.score - a.score || a.c.label.localeCompare(b.c.label));
    return scored.slice(0, limit).map(s => s.c);
}
