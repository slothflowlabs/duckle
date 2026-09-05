"""Generated from crates/duckle-mcp/catalog.json. Do not edit by hand.

Regenerate with: python packaging/pypi/generate_components.py
"""

# id -> {kind, summary, params, unverified}
#   params      keys confirmed present in the engine sources
#   unverified  keys the catalog advertises that no Rust source mentions;
#               still accepted and passed through, never suggested
COMPONENTS = {
    'code.javascript': {
        'kind': 'custom',
        'summary': 'Per-row JS transform via the pure-Rust boa interpreter (sandboxed - no fetch / fs / DOM). Define a `transform(row)` function; the engine calls it per row with the row as a JS object and uses the returned object as the output row. Helpers declared at the top of the script are shared across rows wi...',
        'params': ['routineRef', 'language', 'code', 'cacheOutput'],
    },
    'code.python': {
        'kind': 'custom',
        'summary': 'Transform via a real Python 3 interpreter (full language + installed packages). Define `process(row)` to work a row at a time (a dict in, a dict or None out, JSON both ways), or `transform(table)` to be handed the WHOLE table at once as a pyarrow Table - use that for polars/pandas/PyArrow work, O...',
        'params': ['routineRef', 'language', 'code', 'cacheOutput'],
    },
    'code.shell': {
        'kind': 'custom',
        'summary': 'Run an arbitrary shell command and emit one row with {stdout, stderr, exit_code, duration_ms}. Defaults to cmd.exe on Windows, /bin/sh on Unix. Optional timeout + workingDir. Cancellation kills the child process.',
        'params': ['routineRef', 'language', 'code'],
    },
    'code.sql': {
        'kind': 'custom',
        'summary': 'Run a SELECT; upstream is `input`',
        'params': ['routineRef', 'sql', 'rawSql', 'pureSql', 'loadSpatial', 'loadExtensions'],
    },
    'code.sqltemplate': {
        'kind': 'custom',
        'summary': 'Parameterized SQL with ${context.var}',
        'params': ['routineRef', 'sql', 'rawSql', 'pureSql', 'loadSpatial', 'loadExtensions'],
    },
    'code.wasm': {
        'kind': 'custom',
        'summary': 'Per-row WASM transform via the pure-Rust wasmi interpreter (sandboxed - no fs / net / env access). Supply the module as `wasmB64` (base64) or `path` to a .wasm file. Module must export `memory` and a function `transform(i32, i32) -> i64` packing (out_ptr << 32) | out_len. Defaults: inputColumn=te...',
        'params': ['routineRef', 'language', 'code', 'reuseInstance', 'cacheOutput'],
        'unverified': ['wasmPath'],
    },
    'ctl.anchor': {
        'kind': 'control',
        'summary': 'Does no work itself. It exists so ordering links have something to attach to: wire a trigger out of it to say what runs after, or into it to say what must finish first. Takes no input and produces no rows, so it never joins the data flow.',
        'params': [],
    },
    'ctl.checkpoint': {
        'kind': 'control',
        'summary': 'Pass rows through and also write a parquet snapshot to a path - the durable artifact a future run can read back via src.parquet',
        'params': ['name', 'storage'],
    },
    'ctl.deadletter': {
        'kind': 'control',
        'summary': "Terminal sink for rejected rows - parquet or csv at a configurable path; conventionally wired to an upstream node's reject port",
        'params': ['destination', 'format'],
    },
    'ctl.die': {
        'kind': 'control',
        'summary': 'Stop the pipeline with an error message. Condition controls when it fires: always, only when the input has rows (guard a reject branch), or only when the input is empty (guard missing data).',
        'params': ['message', 'condition'],
    },
    'ctl.file': {
        'kind': 'control',
        'summary': 'One typed filesystem operation: copy, move or delete a file. Staging a file between a landing area and a working area is ordinary batch work; before this the only filesystem-capable component ran a shell command, which cannot serve both platforms from one authored pipeline.',
        'params': ['op', 'source', 'destination', 'overwrite', 'failOnError'],
    },
    'ctl.foreach': {
        'kind': 'control',
        'summary': 'Runs a referenced pipeline once per upstream row. ${ITER_INDEX} + ${ITER_ITEM_<FIELD>} (uppercased) substituted into the sub-pipeline props. Side-effect model.',
        'params': ['pipelineRef', 'itemKey', 'concurrency', 'dispatch', 'maxAttempts', 'retryBackoff', 'retryInitialSeconds', 'retryMaxSeconds'],
    },
    'ctl.iterate': {
        'kind': 'control',
        'summary': "Runs a referenced pipeline N times. Sub-pipeline gets ${ITER_INDEX} (0..N-1) substituted into its props before each call. Side-effect model - sub-pipeline output isn't composed into the parent (true block-scope iteration needs the DAG refactor in docs/dag-block-refactor.md).",
        'params': ['pipelineRef', 'count'],
    },
    'ctl.log': {
        'kind': 'control',
        'summary': 'Emit an info log line, then pass rows through unchanged. Use {rows} in the message for the upstream row count. Lines are written to the run log under the workspace logs/ folder (NDJSON) so Splunk / Dynatrace can ingest them.',
        'params': ['message'],
    },
    'ctl.merge': {
        'kind': 'control',
        'summary': 'Concatenate multiple input streams (UNION ALL)',
        'params': [],
    },
    'ctl.parallelize': {
        'kind': 'control',
        'summary': 'Runs the independent downstream branches wired to its outputs concurrently. The upstream input is snapshotted once and each branch reads that snapshot in its own isolated execution, joining when all finish (any branch failure fails the node).',
        'params': ['maxConcurrency'],
    },
    'ctl.replicate': {
        'kind': 'control',
        'summary': 'Send the same data to multiple downstream outputs',
        'params': [],
    },
    'ctl.retry': {
        'kind': 'control',
        'summary': 'Per-stage retry already lives in the Advanced tab (Retry attempts + Retry backoff) on every node - no separate component needed. A DAG-scoped retry block (wrap N stages, retry the whole group) still needs the DAG-block refactor; use ctl.try with a recovery fallback for now.',
        'params': [],
    },
    'ctl.runevents': {
        'kind': 'control',
        'summary': 'Rows describing the stages that have already failed in this run: node_id, kind, status, message, category, duration_ms. Wire it into a mail or table sink to report failures. It reports failures the run SURVIVED, so mark the stages that may fail with Continue on failure.',
        'params': [],
    },
    'ctl.runjob': {
        'kind': 'control',
        'summary': 'Calls a child pipeline (job) as a side effect, passing parent context variables that are substituted as ${VAR} into the child before it runs. Chain several Run Job nodes to build a Master Job that orchestrates child jobs in sequence. The child runs in its own temp DB; its output is not composed b...',
        'params': ['pipelineRef', 'returnsRows', 'contextVariables'],
    },
    'ctl.runpipeline': {
        'kind': 'control',
        'summary': 'Reads + executes another pipeline file inline as a side effect, then passes the upstream view through unchanged. Useful for triggering helper pipelines (refresh dimension tables, kick off cleanup) without composing their output into the parent.',
        'params': ['pipelineRef', 'returnsRows', 'parameters'],
    },
    'ctl.setvar': {
        'kind': 'control',
        'summary': 'Work out a value while the run is under way and let later steps in the same pipeline ask for it as ${name}: the date on the batch just read, the id just written. Wired to rows the expression is read against them; wired to nothing it stands on its own. The static context cannot carry these, becaus...',
        'params': ['name', 'value'],
    },
    'ctl.switch': {
        'kind': 'control',
        'summary': 'Route rows to case_1..N outputs by condition; first match wins',
        'params': ['branches'],
        'unverified': ['defaultBranch'],
    },
    'ctl.throttle': {
        'kind': 'control',
        'summary': 'Insert an inter-stage delay derived from a rows-per-second target (best-effort for batch pipelines, hook is in place for streaming)',
        'params': ['rate'],
    },
    'ctl.trigger': {
        'kind': 'control',
        'summary': 'Alias of ctl.runpipeline; same executor branch.',
        'params': ['pipelineRef', 'returnsRows', 'parameters'],
    },
    'ctl.try': {
        'kind': 'control',
        'summary': 'Installs a fallback pipeline. If any downstream stage in this execution fails, the fallback runs as a side effect before the original error surfaces - useful for notifications, rollbacks, cleanup. Slice of the DAG-block refactor; true continuation-style try/catch needs the multi-week refactor (se...',
        'params': ['fallbackPipelineRef'],
    },
    'ctl.wait': {
        'kind': 'control',
        'summary': 'Sleep for a fixed number of milliseconds before passing rows through (smoke tests, rate-limit a downstream API)',
        'params': ['duration', 'unit'],
    },
    'ctl.warn': {
        'kind': 'control',
        'summary': 'Emit a warning log line (does not fail the run), then pass rows through. Same {rows} templating and workspace log output as Log Message.',
        'params': ['message'],
    },
    'qa.baseline': {
        'kind': 'quality',
        'summary': 'Compare this run against what previous runs looked like. Every row can satisfy the schema and every row-level rule while the dataset is nothing like what normally arrives - 842,114 rows where five million usually come, a null rate that went from 4 percent to 71, a country partition that vanished ...',
        'params': ['history', 'columns', 'mode', 'rules', 'groupBy', 'requireExistingGroups'],
    },
    'qa.block': {
        'kind': 'quality',
        'summary': 'Cut an entity-resolution job down to the pairs worth comparing. Every fuzzy match compares pairs, and comparing all of them grows with the product of the row counts, so blocking proposes only records that already agree on something cheap and discriminating (same postcode, same surname). One input...',
        'params': ['leftId', 'rightId', 'rules', 'carryColumns'],
    },
    'qa.classify': {
        'kind': 'quality',
        'summary': 'Heuristically classify each column by semantic / PII type - pure regex + statistics, no model. Measures the fraction of values matching known shapes (email, SSN, credit card, IPv4, UUID, URL, phone, date) and tags the best match above a threshold. Emits a report (column, detected_type, match_rate...',
        'params': ['columns', 'threshold'],
    },
    'qa.contract': {
        'kind': 'quality',
        'summary': 'Enforce a data contract: the same rule suite as Expectations (not-null, unique, in-set, in-range, regex, non-negative), but as a GATE. Passes every row through unchanged when all rules hold, and fails the run with a clear error naming the violated rule(s) when any rule breaks. Drop it before a si...',
        'params': ['rules'],
    },
    'qa.dedupe': {
        'kind': 'quality',
        'summary': 'Drop near-duplicate rows by string similarity',
        'params': ['columns', 'threshold', 'algorithm'],
    },
    'qa.describe': {
        'kind': 'quality',
        'summary': 'Column names and types of the input',
        'params': [],
    },
    'qa.expect': {
        'kind': 'quality',
        'summary': 'Run a reusable suite of data-quality expectations (not-null, unique, in-set, in-range, regex, non-negative) and emit a scorecard: one row per rule with total, failed, pass_rate, and passed. The native, no-Python answer to declarative data contracts.',
        'params': ['rules'],
    },
    'qa.freshness': {
        'kind': 'quality',
        'summary': 'Assert the data is recent enough: computes data age = now - max(timestamp column) and checks it against a maxAge (in minutes / hours / days). Gate mode passes every row through unchanged when the freshest row is within the SLA and fails the run with a clear message when it is not. Report mode emi...',
        'params': ['column', 'maxAge', 'maxAgeUnit', 'mode'],
    },
    'qa.geomempty': {
        'kind': 'quality',
        'summary': 'Flag empty geometries with ST_IsEmpty: add an is_empty column (keep all), or keep only empty / only non-empty features.',
        'params': ['geometryColumn', 'mode'],
    },
    'qa.geomrepair': {
        'kind': 'quality',
        'summary': 'Repair invalid geometries in place with ST_MakeValid: fix all geometries, or only the invalid ones (valid features pass through untouched).',
        'params': ['geometryColumn', 'mode'],
    },
    'qa.geomvalidate': {
        'kind': 'quality',
        'summary': 'Flag invalid geometries with ST_IsValid: add an is_valid column (keep all), or keep only valid / only invalid features.',
        'params': ['geometryColumn', 'mode'],
    },
    'qa.histogram': {
        'kind': 'quality',
        'summary': 'Value frequencies for a column',
        'params': ['column'],
    },
    'qa.link': {
        'kind': 'quality',
        'summary': 'Fuzzy-link records across TWO inputs: the main input against a reference on the lookup port. Cross-compares the chosen key columns by string similarity (Jaro-Winkler or Levenshtein) and emits every candidate pair at or above the threshold as left_key, right_key, score. Unlike Record Match (self-j...',
        'params': ['leftColumns', 'rightColumns', 'threshold', 'algorithm'],
    },
    'qa.mask': {
        'kind': 'quality',
        'summary': 'Irreversibly mask a column in place for governance/compliance: deterministic salted-hash pseudonym (joinable across datasets), partial mask (show last N), null-out, or a constant. Pure in-engine, no data leaves your machine.',
        'params': ['column', 'mode', 'salt', 'showLast', 'value'],
    },
    'qa.match': {
        'kind': 'quality',
        'summary': 'Find matching record pairs by similarity, with a score',
        'params': ['columns', 'threshold', 'algorithm'],
    },
    'qa.matchgroup': {
        'kind': 'quality',
        'summary': 'Turn a list of matched record pairs into one stable cluster id per record. Walks the transitive closure of the matches (a~b and b~c put a, b, c in one cluster) and assigns each id the cluster representative (the smallest reachable id). Pairs with Record Match. Output: id, cluster_id.',
        'params': ['leftKey', 'rightKey'],
    },
    'qa.notnull': {
        'kind': 'quality',
        'summary': 'Pass rows with no nulls; rest to reject',
        'params': ['columns', 'onFail'],
    },
    'qa.outlier': {
        'kind': 'quality',
        'summary': 'Pass in-distribution rows; route statistical outliers (IQR or z-score over the chosen numeric column) to the reject port. NULLs and zero-spread data always pass.',
        'params': ['column', 'method', 'sensitivity', 'onFail'],
    },
    'qa.profile': {
        'kind': 'quality',
        'summary': 'Per-column stats: count, nulls, distinct, min/max, quartiles',
        'params': ['columns'],
    },
    'qa.profile.adv': {
        'kind': 'quality',
        'summary': 'Rich single-column profile: count, null_count, null_pct, approx distinct, min/max, the fraction of values matching common patterns (email / integer / decimal / date), and the top-N most frequent values with counts. Long-form output: one row per metric (metric, value, count, pct).',
        'params': ['column', 'topN'],
    },
    'qa.range': {
        'kind': 'quality',
        'summary': 'Pass in-range rows; rest to reject',
        'params': ['column', 'min', 'max', 'inclusive', 'onFail'],
    },
    'qa.reconcile': {
        'kind': 'quality',
        'summary': 'Two-source reconciliation report for migrations and CDC QA. Main input is the source; connect the target to the lookup port. Joins on your key column(s) and emits one row per metric: source_rows, target_rows, rows_only_in_source, rows_only_in_target, keys_matched, plus per measure a source_sum / ...',
        'params': ['keyColumns', 'measureColumns'],
    },
    'qa.refintegrity': {
        'kind': 'quality',
        'summary': 'Check a foreign key against a reference input (connect it to the lookup port): rows whose key exists in the reference pass through, orphan rows (key missing) route to the reject port. Pure semi-join / anti-join, no row fan-out on duplicate reference keys.',
        'params': ['leftKey', 'rightKey'],
    },
    'qa.regex': {
        'kind': 'quality',
        'summary': 'Pass rows matching a pattern; rest to reject',
        'params': ['column', 'pattern', 'onFail'],
    },
    'qa.sample.adv': {
        'kind': 'quality',
        'summary': 'Take a percentage sample of rows. Reservoir (even per-row probability) or Bernoulli (independent per row); set a seed to make the draw reproducible so the same rows are picked every run. All columns are preserved.',
        'params': ['percent', 'method', 'seed'],
    },
    'qa.schemavalidate': {
        'kind': 'quality',
        'summary': 'Reject rows where any expected column is null',
        'params': ['expectedColumns', 'onFail'],
    },
    'qa.standardize': {
        'kind': 'quality',
        'summary': 'Trim, case-normalize, and collapse whitespace',
        'params': ['columns', 'case', 'trim', 'collapseWhitespace'],
    },
    'qa.survivor': {
        'kind': 'quality',
        'summary': 'Collapse duplicate records sharing a key into one golden record, picking each surviving field by rule: most-frequent value, most-recent / oldest (by a date column), or max / min. Applies to every non-key column at once.',
        'params': ['groupBy', 'rule', 'recencyColumn'],
    },
    'qa.unique': {
        'kind': 'quality',
        'summary': 'Pass first per key; duplicates to reject',
        'params': ['columns', 'tieBreak', 'onFail'],
    },
    'snk.avro': {
        'kind': 'sink',
        'summary': "Write rows as an Apache Avro container file via the pure-Rust `apache-avro` crate. Schema is inferred from the first row's column types (long / double / string / boolean) - or supply a JSON Avro schema via the schemaJson field to override. recordName names the inferred record (default `Row`).",
        'params': ['path', 'mode', 'compression'],
    },
    'snk.azureblob': {
        'kind': 'sink',
        'summary': 'Write via the azure extension',
        'params': ['bucket', 'key', 'region', 'accessKey', 'secretKey', 'connectionRef', 'endpoint', 'urlStyle', 'useSsl', 'format', 'mode', 'compression', 'partitionBy'],
    },
    'snk.b2': {
        'kind': 'sink',
        'summary': 'Write via S3-compatible endpoint',
        'params': ['bucket', 'key', 'region', 'accessKey', 'secretKey', 'connectionRef', 'endpoint', 'urlStyle', 'useSsl', 'format', 'mode', 'compression', 'partitionBy'],
    },
    'snk.bigquery': {
        'kind': 'sink',
        'summary': 'Write tables to BigQuery via the duckdb-bigquery community extension',
        'params': ['project', 'dataset', 'schemaName', 'tableName', 'mode', 'credentialsPath', 'validateBeforeInsert', 'deadLetterPath', 'deadLetterFormat'],
    },
    'snk.cassandra': {
        'kind': 'sink',
        'summary': 'INSERT rows into a Cassandra table via the scylla CQL driver (one INSERT per row; CQL has no multi-row VALUES).',
        'params': ['contactPoints', 'user', 'password', 'keyspace', 'tableName', 'batchSize'],
    },
    'snk.chroma': {
        'kind': 'sink',
        'summary': '',
        'params': ['endpoint', 'apiKey', 'collection', 'connectionRef', 'embeddingColumn', 'idColumn', 'dimension', 'metric', 'mode', 'batchSize'],
        'unverified': ['metadataColumns', 'createIfMissing'],
    },
    'snk.clickhouse': {
        'kind': 'sink',
        'summary': 'INSERT to ClickHouse via the HTTP interface (FORMAT JSONEachRow). Batched at 10k rows by default.',
        'params': ['endpoint', 'user', 'password', 'database', 'tableName', 'batchSize'],
    },
    'snk.cockroach': {
        'kind': 'sink',
        'summary': 'Write to CockroachDB via the DuckDB postgres extension (Cockroach speaks the Postgres wire protocol)',
        'params': ['connectionRef', 'host', 'port', 'database', 'username', 'password', 'sslmode', 'sslrootcert', 'sslcert', 'sslkey', 'connectTimeout', 'options', 'connParams', 'schemaName', 'tableName', 'mode', 'conflictColumns', 'deleteColumn', 'deleteValue', 'validateBeforeInsert', 'deadLetterPath', 'deadLetterFormat'],
    },
    'snk.csv': {
        'kind': 'sink',
        'summary': '',
        'params': ['path', 'mode', 'delimiter', 'writeHeader', 'encoding', 'nullValue', 'partitionBy'],
    },
    'snk.databricks': {
        'kind': 'sink',
        'summary': 'INSERT to a Databricks table via the Statement Execution API with PAT Bearer auth. Multi-row INSERTs batched at 1000 rows; sync wait up to 50s.',
        'params': ['workspace', 'pat', 'warehouseId', 'catalog', 'schema', 'tableName', 'batchSize', 'waitTimeoutSeconds', 'mode', 'conflictColumns', 'deleteColumn', 'deleteValue'],
    },
    'snk.db2': {
        'kind': 'sink',
        'summary': 'Write to IBM DB2 through the IBM Data Server ODBC driver. Creates the table if missing from the upstream column types; Append adds rows, Overwrite clears it first. Booleans land in SMALLINT as 1/0, which DB2 for z/OS also accepts. No upsert.',
        'params': ['host', 'port', 'database', 'user', 'password', 'useSsl', 'driver', 'dsn', 'connectionString', 'schema', 'tableName', 'mode'],
    },
    'snk.dhis2': {
        'kind': 'sink',
        'summary': 'Import rows into DHIS2. Set url to https://<host>/api/dataValueSets (importType aggregate) or https://<host>/api/tracker (importType tracker + trackerResource trackedEntities/events/enrollments/relationships). Rows are chunked (chunkSize, default 1000) and wrapped in the collection key DHIS2 expe...',
        'params': ['url', 'importType', 'trackerResource', 'importStrategy', 'chunkSize', 'dryRun', 'atomicMode', 'failOnConflict', 'authType', 'authHeader', 'authToken'],
    },
    'snk.duckdb': {
        'kind': 'sink',
        'summary': 'Write a table into a DuckDB file',
        'params': ['database', 'tableName', 'mode', 'conflictColumns', 'deleteColumn', 'deleteValue', 'validateBeforeInsert', 'deadLetterPath', 'deadLetterFormat'],
    },
    'snk.ducklake': {
        'kind': 'sink',
        'summary': 'Write a table into a DuckLake catalog',
        'params': ['path', 'dataPath', 'metadataSchema', 'attachOptions', 'schemaName', 'tableName', 'publishGroup', 'mode', 'conflictColumns', 'validateBeforeInsert', 'deadLetterPath', 'deadLetterFormat'],
    },
    'snk.elastic': {
        'kind': 'sink',
        'summary': 'Bulk-index docs via the _bulk NDJSON API (configurable host, index, ApiKey auth)',
        'params': ['endpoint', 'index', 'apiKey'],
        'unverified': ['shapeHint'],
    },
    'snk.email': {
        'kind': 'sink',
        'summary': 'Per-row SMTP send via pure-Rust `lettre` + rustls TLS. Props: host (required), port (default 587), user/password (optional - skip for relay-only servers), fromAddress (required), toColumn (default `to`), subjectColumn (default `subject`), bodyColumn (default `body`). Plain text only for v1; HTML ...',
        'params': ['host', 'port', 'user', 'password', 'fromAddress', 'toColumn', 'subjectColumn', 'bodyColumn', 'to', 'subject', 'body'],
    },
    'snk.excel': {
        'kind': 'sink',
        'summary': 'Write .xlsx via the DuckDB excel extension',
        'params': ['path', 'mode', 'compression', 'hasHeader'],
    },
    'snk.execsource': {
        'kind': 'sink',
        'summary': 'In-database processing: run a CREATE TABLE AS query on the source server itself (Postgres / MySQL) via postgres_execute / mysql_execute. The transform executes in the database and the result lands there, with no round-trip through DuckDB. Self-contained: no input needed.',
        'params': ['engine', 'host', 'port', 'database', 'username', 'password', 'connString', 'sql', 'destSchema', 'destTable', 'mode'],
    },
    'snk.ftp': {
        'kind': 'sink',
        'summary': 'Upload pipeline output over FTP / FTPS / SFTP',
        'params': ['protocol', 'host', 'port', 'user', 'password', 'privateKey', 'keyPassphrase', 'hostFingerprint', 'remotePath', 'format'],
    },
    'snk.gcs': {
        'kind': 'sink',
        'summary': 'Write via DuckDB httpfs',
        'params': ['bucket', 'key', 'region', 'accessKey', 'secretKey', 'connectionRef', 'endpoint', 'urlStyle', 'useSsl', 'format', 'mode', 'compression', 'partitionBy'],
    },
    'snk.gizmosql': {
        'kind': 'sink',
        'summary': 'Write rows to a table on a GizmoSQL (Arrow Flight SQL) server via CREATE + batched INSERT over the clean-room pure-Rust Flight SQL client. Append or overwrite; TLS optional.',
        'params': ['host', 'port', 'username', 'password', 'tls', 'tlsSkipVerify', 'table', 'mode'],
    },
    'snk.graphql': {
        'kind': 'sink',
        'summary': 'POST a GraphQL mutation per upstream row. The mutation body can reference row fields via ${field} substitution.',
        'params': ['url', 'method', 'headers', 'batchMode', 'bodyType', 'bodyTemplate', 'authType', 'authToken', 'authHeader'],
    },
    'snk.huggingface': {
        'kind': 'sink',
        'summary': 'Push the pipeline output to a Hugging Face Hub dataset repo. The engine materializes a Parquet and commits it over the Hub API (create-repo -> preupload -> git-LFS -> commit). Needs a write-scoped token; the repo is created if it does not exist.',
        'params': ['repo', 'path', 'token', 'private', 'revision', 'commitMessage'],
    },
    'snk.iceberg': {
        'kind': 'sink',
        'summary': 'Write a full Iceberg table (data/ + metadata/) via DuckDB v1.5',
        'params': ['path'],
    },
    'snk.json': {
        'kind': 'sink',
        'summary': '',
        'params': ['path', 'mode', 'compression', 'format', 'flatten', 'keepParentNames', 'sampleSize', 'recordsPath'],
    },
    'snk.jsonl': {
        'kind': 'sink',
        'summary': '',
        'params': ['path', 'mode', 'compression', 'format', 'flatten', 'keepParentNames', 'sampleSize', 'recordsPath'],
    },
    'snk.kafka': {
        'kind': 'sink',
        'summary': 'Produce one Kafka record per upstream row via the pure-Rust `rskafka` driver. Record key = optional keyColumn value; record value = JSON-stringified row. Records go to a single partition (partitionId, default 0); pipelined batching (default 500 records per produce call). Every write is acknowledg...',
        'params': ['brokers', 'topic', 'format', 'keyColumn'],
    },
    'snk.lancedb': {
        'kind': 'sink',
        'summary': 'Write rows to a Lance table (create/overwrite or append) via the bundled duckle-lance sidecar.',
        'params': ['uri', 'table', 'mode', 'apiKey', 'region'],
    },
    'snk.mariadb': {
        'kind': 'sink',
        'summary': 'Write to MariaDB via the DuckDB mysql extension (MariaDB speaks the MySQL wire protocol)',
        'params': ['connectionRef', 'host', 'port', 'database', 'username', 'password', 'schemaName', 'tableName', 'mode', 'conflictColumns', 'deleteColumn', 'deleteValue', 'validateBeforeInsert', 'deadLetterPath', 'deadLetterFormat'],
    },
    'snk.milvus': {
        'kind': 'sink',
        'summary': 'Insert rows to a Milvus collection via /v1/vector/insert',
        'params': ['endpoint', 'collection', 'apiKey'],
        'unverified': ['shapeHint'],
    },
    'snk.minio': {
        'kind': 'sink',
        'summary': 'Write via S3-compatible endpoint',
        'params': ['bucket', 'key', 'region', 'accessKey', 'secretKey', 'connectionRef', 'endpoint', 'urlStyle', 'useSsl', 'format', 'mode', 'compression', 'partitionBy'],
    },
    'snk.model': {
        'kind': 'sink',
        'summary': 'Register a trained model. The card IS the upstream row - the artifact URI your training script wrote, plus whatever metrics, framework and hashes it recorded - written to <path>/<name>/<version>.json with a latest.json pointer beside it. It needs exactly one row and a version column. The write ha...',
        'params': ['name', 'path'],
    },
    'snk.mongodb': {
        'kind': 'sink',
        'summary': 'Insert documents into a MongoDB collection via the official driver. Bulk insert_many batched at 1000 docs by default; replace mode drops the collection first.',
        'params': ['uri', 'database', 'collection', 'mode', 'conflictColumns', 'deleteColumn', 'deleteValue', 'batchSize'],
    },
    'snk.motherduck': {
        'kind': 'sink',
        'summary': 'Write a table into MotherDuck via ATTACH md:',
        'params': ['database', 'token', 'schemaName', 'tableName', 'mode', 'conflictColumns', 'validateBeforeInsert', 'deadLetterPath', 'deadLetterFormat'],
    },
    'snk.mysql': {
        'kind': 'sink',
        'summary': 'Write to MySQL via the DuckDB mysql extension',
        'params': ['connectionRef', 'host', 'port', 'database', 'username', 'password', 'schemaName', 'tableName', 'mode', 'conflictColumns', 'deleteColumn', 'deleteValue', 'validateBeforeInsert', 'deadLetterPath', 'deadLetterFormat'],
    },
    'snk.nats': {
        'kind': 'sink',
        'summary': 'Publish each upstream row as one NATS message via the pure-Rust `async-nats` driver. Payload = JSON-stringified row. Optional subjectSuffixColumn appends a per-row suffix (subject.value) for routed multi-tenant publishing.',
        'params': ['urls', 'subject', 'subjectSuffixColumn', 'batchSize'],
    },
    'snk.neo4j': {
        'kind': 'sink',
        'summary': 'Write rows as Neo4j nodes over the HTTP Query API. Rows ride up as one $rows parameter expanded with UNWIND, so a batch is one round trip. Set mergeKeys to MERGE on those properties (re-running updates the matched nodes) instead of CREATE; or supply your own Cypher that consumes $rows.',
        'params': ['endpoint', 'database', 'user', 'password', 'label', 'mergeKeys', 'batchSize', 'cypher'],
    },
    'snk.opensearch': {
        'kind': 'sink',
        'summary': 'Bulk-index docs via the OpenSearch _bulk NDJSON API (same shape as Elasticsearch)',
        'params': ['endpoint', 'index', 'apiKey'],
        'unverified': ['shapeHint'],
    },
    'snk.oracle': {
        'kind': 'sink',
        'summary': 'INSERT to Oracle via the official `oracle` Rust crate. Built into the shipped binary - users need Oracle Instant Client on the library path at runtime. Multi-row INSERT ALL ... SELECT 1 FROM dual idiom batched at 1000 rows.',
        'params': ['connect', 'user', 'password', 'schema', 'tableName', 'batchSize', 'mode', 'conflictColumns', 'deleteColumn', 'deleteValue'],
        'unverified': ['oracleRuntimeNote'],
    },
    'snk.parquet': {
        'kind': 'sink',
        'summary': '',
        'params': ['path', 'mode', 'compression', 'compressionLevel', 'parquetVersion', 'rowGroupSize', 'partitionBy', 'maxPartitions', 'hilbertColumn'],
    },
    'snk.pgvector': {
        'kind': 'sink',
        'summary': 'Write embeddings to a Postgres table (server must have CREATE EXTENSION vector)',
        'params': ['connectionRef', 'host', 'port', 'database', 'username', 'password', 'sslmode', 'sslrootcert', 'sslcert', 'sslkey', 'connectTimeout', 'options', 'connParams', 'schemaName', 'tableName', 'mode', 'conflictColumns', 'validateBeforeInsert', 'deadLetterPath', 'deadLetterFormat'],
    },
    'snk.pinecone': {
        'kind': 'sink',
        'summary': 'Upsert vectors to a Pinecone index via /vectors/upsert with Api-Key auth',
        'params': ['indexHost', 'apiKey'],
        'unverified': ['shapeHint'],
    },
    'snk.pixeltable': {
        'kind': 'sink',
        'summary': 'Write rows into a Pixeltable table (#223). Duckle COPYs the upstream rows to Parquet and Pixeltable inserts the file directly. Insert appends to an existing table; Create builds one from the incoming rows. Needs a Python with pixeltable installed; the desktop app provisions one on first use.',
        'params': ['table', 'mode'],
    },
    'snk.postgres': {
        'kind': 'sink',
        'summary': 'Write to PostgreSQL via the DuckDB postgres extension',
        'params': ['connectionRef', 'host', 'port', 'database', 'username', 'password', 'sslmode', 'sslrootcert', 'sslcert', 'sslkey', 'connectTimeout', 'options', 'connParams', 'schemaName', 'tableName', 'mode', 'conflictColumns', 'deleteColumn', 'deleteValue', 'validateBeforeInsert', 'deadLetterPath', 'deadLetterFormat'],
    },
    'snk.pubsub': {
        'kind': 'sink',
        'summary': 'Publish messages via the Pub/Sub REST API (POST /v1/projects/{p}/topics/{t}:publish). Each upstream row -> one base64-encoded message. Auth via OAuth2 Bearer access token. Batched at 100 messages per request (Pub/Sub max).',
        'params': ['project', 'topic', 'accessToken', 'batchSize'],
    },
    'snk.qdrant': {
        'kind': 'sink',
        'summary': 'Upsert points to a Qdrant collection via PUT /collections/{name}/points',
        'params': ['clusterUrl', 'collection', 'apiKey'],
        'unverified': ['shapeHint'],
    },
    'snk.quack': {
        'kind': 'sink',
        'summary': 'Write a table to a remote DuckDB instance over the Quack protocol (HTTP on port 9494). Supports append / overwrite / truncate / upsert modes via the standard relational sink path.',
        'params': ['host', 'port', 'token', 'schemaName', 'tableName', 'mode', 'validateBeforeInsert', 'deadLetterPath', 'deadLetterFormat'],
    },
    'snk.qvd': {
        'kind': 'sink',
        'summary': 'Write rows as a Qlik QVD file (.qvd) via a clean-room pure-Rust encoder (no Qlik runtime). Builds the per-column symbol tables + bit-stuffed index; values are typed per cell (int / double / string), nulls preserved. Round-trips with the QVD source and loads in QlikView / Qlik Sense.',
        'params': ['path', 'mode', 'compression'],
    },
    'snk.r2': {
        'kind': 'sink',
        'summary': 'Write via S3-compatible endpoint',
        'params': ['bucket', 'key', 'region', 'accessKey', 'secretKey', 'connectionRef', 'endpoint', 'urlStyle', 'useSsl', 'format', 'mode', 'compression', 'partitionBy'],
    },
    'snk.rabbit': {
        'kind': 'sink',
        'summary': 'Publish each upstream row as one persistent-delivery-mode AMQP 0.9.1 message via the pure-Rust `lapin` driver. Configurable exchange + routingKey; empty exchange = default direct exchange (route to queue named by routingKey).',
        'params': ['url', 'routingKey', 'exchange', 'batchSize'],
    },
    'snk.redis': {
        'kind': 'sink',
        'summary': "SET each row's keyColumn -> valueColumn into Redis via the sync `redis` Rust client. Optional ttlSeconds adds an EXPIRE. If valueColumn is empty, the whole row is JSON-stringified as the value. Pipelined in chunks (default 1000).",
        'params': ['connectionString', 'keyColumn', 'valueColumn', 'ttlSeconds', 'batchSize'],
    },
    'snk.redpanda': {
        'kind': 'sink',
        'summary': 'Same wire protocol as Kafka - rides the rskafka driver. Use snk.kafka semantics.',
        'params': ['brokers', 'topic', 'format', 'keyColumn'],
    },
    'snk.redshift': {
        'kind': 'sink',
        'summary': 'Write Redshift via the postgres ATTACH path (Postgres wire on port 5439); overwrite / append / truncate / upsert all supported via the existing PG sink modes',
        'params': ['connectionRef', 'host', 'port', 'database', 'username', 'password', 'sslmode', 'sslrootcert', 'sslcert', 'sslkey', 'connectTimeout', 'options', 'connParams', 'schemaName', 'tableName', 'mode', 'conflictColumns', 'validateBeforeInsert', 'deadLetterPath', 'deadLetterFormat'],
    },
    'snk.rest': {
        'kind': 'sink',
        'summary': 'HTTP POST one batched request containing the result as a JSON array (configurable method, headers, body shape)',
        'params': ['url', 'method', 'headers', 'batchMode', 'bodyType', 'bodyTemplate', 'authType', 'authToken', 'authHeader'],
    },
    'snk.s3': {
        'kind': 'sink',
        'summary': 'Write via DuckDB httpfs',
        'params': ['path', 'connectionRef', 'format', 'accessKey', 'secretKey', 'region', 'compression', 'compressionLevel', 'parquetVersion', 'rowGroupSize', 'delimiter', 'writeHeader', 'nullValue', 'endpoint', 'urlStyle', 'useSsl'],
    },
    'snk.salesforce': {
        'kind': 'sink',
        'summary': 'Write rows into a Salesforce object via the REST sObject Collections API (<=200 records/request). insert / update / upsert (by external Id) / delete; auth is a Bearer token or OAuth 2.0 client-credentials (a fresh token minted per run from a connected app). For migration-scale loads use Salesforc...',
        'params': ['connectionRef', 'authMode', 'instanceUrl', 'accessToken', 'loginUrl', 'clientId', 'clientSecret', 'apiVersion', 'object', 'operation', 'externalIdField', 'idField', 'batchSize', 'allOrNone', 'failOnError', 'resultsPath'],
    },
    'snk.salesforce.bulk': {
        'kind': 'sink',
        'summary': 'Write rows into a Salesforce object via Bulk API 2.0 - the migration-scale path. DuckDB streams the upstream to CSV on disk and each <=90MB part runs as an async job (insert / update / upsert / delete / hardDelete). Same Bearer / OAuth client-credentials auth as Salesforce. Result sets are writte...',
        'params': ['connectionRef', 'authMode', 'instanceUrl', 'accessToken', 'loginUrl', 'clientId', 'clientSecret', 'apiVersion', 'object', 'operation', 'externalIdField', 'idField', 'assignmentRuleId', 'failOnError', 'resultsPath', 'pollIntervalSecs', 'timeoutSecs'],
    },
    'snk.scylla': {
        'kind': 'sink',
        'summary': 'Same wire as snk.cassandra - INSERT via the scylla CQL driver.',
        'params': ['contactPoints', 'user', 'password', 'keyspace', 'tableName', 'batchSize'],
    },
    'snk.snowflake': {
        'kind': 'sink',
        'summary': 'INSERT to a Snowflake table via the SQL API (/api/v2/statements) with PAT (Personal Access Token) bearer auth. Multi-row INSERTs batched at 1000 rows by default.',
        'params': ['account', 'authType', 'pat', 'user', 'privateKeyPath', 'warehouse', 'role', 'database', 'schema', 'tableName', 'batchSize', 'mode', 'conflictColumns', 'deleteColumn', 'deleteValue'],
    },
    'snk.spatial': {
        'kind': 'sink',
        'summary': 'Write geospatial files via the spatial extension',
        'params': ['path', 'driver', 'encoding'],
    },
    'snk.sqlite': {
        'kind': 'sink',
        'summary': 'Write a table into a SQLite file',
        'params': ['database', 'tableName', 'mode', 'conflictColumns', 'deleteColumn', 'deleteValue', 'validateBeforeInsert', 'deadLetterPath', 'deadLetterFormat'],
    },
    'snk.sqlserver': {
        'kind': 'sink',
        'summary': 'INSERT to SQL Server via TDS (multi-row VALUES batched at 1000 rows, the SQL Server cap).',
        'params': ['connectionRef', 'host', 'port', 'user', 'password', 'database', 'trustCert', 'encrypt', 'bulk', 'schema', 'tableName', 'batchSize', 'mode', 'conflictColumns', 'deleteColumn', 'deleteValue', 'validateBeforeInsert', 'deadLetterPath', 'deadLetterFormat'],
    },
    'snk.synapse': {
        'kind': 'sink',
        'summary': 'Azure Synapse rides the SQL Server TDS wire - same connection form as snk.sqlserver.',
        'params': ['connectionRef', 'host', 'port', 'user', 'password', 'database', 'trustCert', 'encrypt', 'bulk', 'schema', 'tableName', 'batchSize', 'mode', 'conflictColumns', 'deleteColumn', 'deleteValue', 'validateBeforeInsert', 'deadLetterPath', 'deadLetterFormat'],
    },
    'snk.teradata': {
        'kind': 'sink',
        'summary': 'Write to Teradata through its free ODBC driver. Install the Teradata ODBC driver, then connect with friendly fields, a DSN, or a full ODBC connection string. Append creates the table if missing then appends; Overwrite clears it first. No upsert.',
        'params': ['driver', 'host', 'user', 'password', 'dsn', 'connectionString', 'tableName', 'database', 'writeMode'],
    },
    'snk.toml': {
        'kind': 'sink',
        'summary': 'Write the upstream rows as TOML. TOML disallows a top-level array so the engine wraps under a `rows` key: `[[rows]]` per row.',
        'params': ['path', 'mode', 'compression'],
    },
    'snk.tsv': {
        'kind': 'sink',
        'summary': 'Write tab-separated files',
        'params': ['path', 'mode', 'compression', 'hasHeader', 'delimiter', 'quoteChar', 'skipLines', 'partitionBy'],
    },
    'snk.turso': {
        'kind': 'sink',
        'summary': 'INSERT rows into a Turso (libSQL) database over the HTTP pipeline API. Creates the table if missing from the upstream column types; Append adds rows, Overwrite clears it first. Values go up as bound parameters, batched (default 500).',
        'params': ['url', 'authToken', 'tableName', 'mode', 'batchSize'],
    },
    'snk.vortex': {
        'kind': 'sink',
        'summary': 'Write rows as a Vortex columnar file (.vortex) via the bundled duckle-lance sidecar. Next-gen columnar format with fast random access; the engine bridges the upstream rows through Parquet into Vortex.',
        'params': ['path'],
    },
    'snk.weaviate': {
        'kind': 'sink',
        'summary': 'Batch upsert objects to a Weaviate cluster via /v1/batch/objects with Bearer auth',
        'params': ['endpoint', 'apiKey'],
        'unverified': ['shapeHint'],
    },
    'snk.webhook': {
        'kind': 'sink',
        'summary': 'HTTP POST one request per row, body = row JSON (configurable method + headers)',
        'params': ['url', 'method', 'headers', 'batchMode', 'bodyType', 'bodyTemplate', 'authType', 'authToken', 'authHeader'],
    },
    'snk.websocket': {
        'kind': 'sink',
        'summary': 'Connect to a ws:// or wss:// URL and send each upstream row as a text frame - the whole row as JSON, or one column when messageColumn is set - then close. For pushing processed results to real-time dashboards or WebSocket APIs.',
        'params': ['url', 'messageColumn', 'headers'],
    },
    'snk.xml': {
        'kind': 'sink',
        'summary': 'Write rows as XML via `quick-xml`. Default shape: `<root><row><col>val</col>...</row>...</root>`. rootElement / rowElement override the wrapper names. Complex (object/array) cell values are JSON-encoded inside CDATA so the file round-trips back through src.xml losslessly.',
        'params': ['path', 'mode', 'compression', 'rowPath', 'namespace'],
    },
    'snk.yaml': {
        'kind': 'sink',
        'summary': 'Write the upstream rows as a top-level YAML array (`- key: value` per row).',
        'params': ['path', 'mode', 'compression'],
    },
    'src.adbc': {
        'kind': 'source',
        'summary': 'Read any database that ships an ADBC (Arrow Database Connectivity) driver. Point at a prebuilt driver shared library (.dll / .so / .dylib) plus a connection URI and SQL; rows stream back as Arrow for fast loads. Friendly wrappers can map their own fields onto driver / options.',
        'params': ['driver', 'entrypoint', 'uri', 'options', 'query'],
    },
    'src.airtable': {
        'kind': 'source',
        'summary': 'Airtable REST. Bearer Personal Access Token. Cursor pagination on `offset` (cursorNextPath /offset, cursorParam `offset`). responsePath /records.',
        'params': ['url', 'method', 'body', 'headers', 'connectionRef', 'transportRef', 'authType', 'authToken', 'authHeader', 'tokenUrl', 'clientId', 'clientSecret', 'clientAuth', 'scope', 'responsePath', 'jsonPath', 'paginationType', 'cursorNextPath', 'cursorParam', 'offsetParam', 'pageSize', 'totalCountPath', 'pageParam', 'startPage', 'maxPages', 'urlTemplate', 'parentKeyColumn', 'maxRequests', 'incrementalField', 'incrementalInitial', 'concurrency', 'checkpoint', 'onParentError', 'responseMetadata', 'rawResponseDestination', 'httpProxy', 'httpUserAgent', 'httpConnectTimeoutSecs', 'httpReadTimeoutSecs'],
    },
    'src.artifact': {
        'kind': 'source',
        'summary': 'One row per FILE described the way a pipeline can reason about it: uri, name, media_type, size_bytes, sha256 and modified_at. For PDFs, images, archives, OCR output and model binaries - an artifact is a reference, not the bytes, so it joins, filters and iterates like any other table. Hashing is o...',
        'params': ['path', 'glob', 'recursive', 'hash'],
    },
    'src.asana': {
        'kind': 'source',
        'summary': 'Asana REST. Bearer Personal Access Token (https://app.asana.com/0/my-apps). Cursor pagination on `next_page.offset` (cursorNextPath /next_page/offset, cursorParam `offset`). responsePath /data. Base URL https://app.asana.com/api/1.0.',
        'params': ['url', 'method', 'body', 'headers', 'connectionRef', 'transportRef', 'authType', 'authToken', 'authHeader', 'tokenUrl', 'clientId', 'clientSecret', 'clientAuth', 'scope', 'responsePath', 'jsonPath', 'paginationType', 'cursorNextPath', 'cursorParam', 'offsetParam', 'pageSize', 'totalCountPath', 'pageParam', 'startPage', 'maxPages', 'urlTemplate', 'parentKeyColumn', 'maxRequests', 'incrementalField', 'incrementalInitial', 'concurrency', 'checkpoint', 'onParentError', 'responseMetadata', 'rawResponseDestination', 'httpProxy', 'httpUserAgent', 'httpConnectTimeoutSecs', 'httpReadTimeoutSecs'],
    },
    'src.avro': {
        'kind': 'source',
        'summary': "Apache Avro container files (.avro / .ocf) via the pure-Rust `apache-avro` crate. The file carries its own schema; engine doesn't need any schema config. Pairs with Kafka topics that publish Avro-encoded payloads.",
        'params': ['path', 'encoding', 'glob'],
    },
    'src.azureblob': {
        'kind': 'source',
        'summary': 'Read via the azure extension',
        'params': ['bucket', 'key', 'region', 'glob', 'accessKey', 'secretKey', 'sessionToken', 'connectionRef', 'endpoint', 'urlStyle', 'useSsl', 'format', 'hasHeader', 'delimiter', 'quoteChar', 'encoding', 'skipLines', 'nullValue', 'nullPadding', 'ignoreErrors', 'readOptions', 'recordsPath', 'flatten', 'keepParentNames'],
    },
    'src.b2': {
        'kind': 'source',
        'summary': 'Read via S3-compatible endpoint',
        'params': ['bucket', 'key', 'region', 'glob', 'accessKey', 'secretKey', 'sessionToken', 'connectionRef', 'endpoint', 'urlStyle', 'useSsl', 'format', 'hasHeader', 'delimiter', 'quoteChar', 'encoding', 'skipLines', 'nullValue', 'nullPadding', 'ignoreErrors', 'readOptions', 'recordsPath', 'flatten', 'keepParentNames'],
    },
    'src.bigquery': {
        'kind': 'source',
        'summary': 'Read tables from BigQuery via the duckdb-bigquery community extension - uses standard GCP credential discovery',
        'params': ['project', 'dataset', 'schemaName', 'tableName', 'query', 'credentialsPath'],
    },
    'src.cassandra': {
        'kind': 'source',
        'summary': 'Read CQL via the scylla driver (works with both Cassandra and ScyllaDB).',
        'params': ['contactPoints', 'user', 'password', 'keyspace', 'tableName', 'query'],
    },
    'src.changed': {
        'kind': 'source',
        'summary': 'Poll a remote source METADATA and emit a row only for what changed - a HEAD or an SFTP stat costs nothing next to the object it decides about. Object mode watches one URI; listing mode watches an s3:// prefix or an sftp:// directory of immutable files and emits the new and changed ones for a ForE...',
        'params': ['uri', 'listing', 'suffix', 'maxEntries', 'trackState', 'user', 'password', 'privateKey', 'keyPassphrase', 'hostFingerprint', 'headers', 'accessKey', 'secretKey', 'sessionToken', 'region', 'endpoint', 'urlStyle', 'useSsl'],
    },
    'src.chroma': {
        'kind': 'source',
        'summary': '',
        'params': ['endpoint', 'apiKey', 'collection', 'connectionRef', 'topK', 'filter'],
        'unverified': ['queryMode', 'queryText'],
    },
    'src.clickhouse': {
        'kind': 'source',
        'summary': 'Read ClickHouse via the HTTP interface (POST SELECT ... FORMAT JSON). User/password auth via X-ClickHouse-User / X-ClickHouse-Key headers.',
        'params': ['endpoint', 'user', 'password', 'database', 'tableName', 'query'],
    },
    'src.clickup': {
        'kind': 'source',
        'summary': 'ClickUp REST. Bearer Personal API token (pk_... from Settings > Apps). Page pagination on `?page=N` (paginationType `page`, pageParam `page`). responsePath /tasks (or whatever resource). Base URL https://api.clickup.com/api/v2.',
        'params': ['url', 'method', 'body', 'headers', 'connectionRef', 'transportRef', 'authType', 'authToken', 'authHeader', 'tokenUrl', 'clientId', 'clientSecret', 'clientAuth', 'scope', 'responsePath', 'jsonPath', 'paginationType', 'cursorNextPath', 'cursorParam', 'offsetParam', 'pageSize', 'totalCountPath', 'pageParam', 'startPage', 'maxPages', 'urlTemplate', 'parentKeyColumn', 'maxRequests', 'incrementalField', 'incrementalInitial', 'concurrency', 'checkpoint', 'onParentError', 'responseMetadata', 'rawResponseDestination', 'httpProxy', 'httpUserAgent', 'httpConnectTimeoutSecs', 'httpReadTimeoutSecs'],
    },
    'src.clipboard': {
        'kind': 'source',
        'summary': 'Read the system clipboard via pure-Rust arboard. If the text parses as JSON-array-of-objects each element becomes a row; otherwise a single {text, length} row is emitted. Fails clearly on headless Linux (no display server) - desktop-only by design.',
        'params': [],
    },
    'src.cockroach': {
        'kind': 'source',
        'summary': 'Read from CockroachDB via the DuckDB postgres extension',
        'params': ['connectionRef', 'host', 'port', 'database', 'username', 'password', 'sslmode', 'sslrootcert', 'sslcert', 'sslkey', 'connectTimeout', 'options', 'connParams', 'mode', 'schemaName', 'tableName', 'sql', 'pushdown', 'readOnly', 'connString'],
        'unverified': ['fetchSize'],
    },
    'src.couchdb': {
        'kind': 'source',
        'summary': 'Read CouchDB documents via the _all_docs endpoint (include_docs=true). Rides src.rest - Basic auth, responsePath /rows, cursor pagination via `next_key` if configured.',
        'params': ['url', 'method', 'body', 'headers', 'connectionRef', 'transportRef', 'authType', 'authToken', 'authHeader', 'tokenUrl', 'clientId', 'clientSecret', 'clientAuth', 'scope', 'responsePath', 'jsonPath', 'paginationType', 'cursorNextPath', 'cursorParam', 'offsetParam', 'pageSize', 'totalCountPath', 'pageParam', 'startPage', 'maxPages', 'urlTemplate', 'parentKeyColumn', 'maxRequests', 'incrementalField', 'incrementalInitial', 'concurrency', 'checkpoint', 'onParentError', 'responseMetadata', 'rawResponseDestination', 'httpProxy', 'httpUserAgent', 'httpConnectTimeoutSecs', 'httpReadTimeoutSecs'],
    },
    'src.csv': {
        'kind': 'source',
        'summary': 'Read delimited text files',
        'params': ['path', 'hasHeader', 'delimiter', 'quoteChar', 'encoding', 'skipLines', 'nullValue', 'nullPadding', 'ignoreErrors', 'readOptions'],
    },
    'src.databricks': {
        'kind': 'source',
        'summary': 'Read Databricks via the SQL Statement Execution API with PAT Bearer auth. Engine materializes inline result sets as a DuckDB table for downstream stages.',
        'params': ['workspace', 'pat', 'warehouseId', 'catalog', 'schema', 'tableName', 'query', 'waitTimeoutSeconds'],
    },
    'src.db2': {
        'kind': 'source',
        'summary': 'Read IBM DB2 through the IBM Data Server ODBC driver (DB2 ships no DuckDB extension and no native Rust driver). Install the IBM driver, then connect with friendly host / port / database / user / password fields, a DSN, or a full ODBC connection string. Whole-table read or custom SQL; types preser...',
        'params': ['host', 'port', 'database', 'user', 'password', 'useSsl', 'driver', 'dsn', 'connectionString', 'schema', 'tableName', 'query', 'batchSize'],
    },
    'src.delta': {
        'kind': 'source',
        'summary': 'Read Delta Lake tables via DuckDB delta_scan',
        'params': ['path'],
    },
    'src.dhis2': {
        'kind': 'source',
        'summary': 'DHIS2 Web API source - thin alias over src.rest. Auth: pick API key, set authHeader to Authorization, and put "ApiToken d2pat_..." (2.37+) or "Basic <user:password>" in the token field; plain Basic works too. Set responsePath per endpoint, since DHIS2 uses a different envelope for each: /api/data...',
        'params': ['url', 'method', 'body', 'headers', 'connectionRef', 'transportRef', 'authType', 'authToken', 'authHeader', 'tokenUrl', 'clientId', 'clientSecret', 'clientAuth', 'scope', 'responsePath', 'jsonPath', 'paginationType', 'cursorNextPath', 'cursorParam', 'offsetParam', 'pageSize', 'totalCountPath', 'pageParam', 'startPage', 'maxPages', 'urlTemplate', 'parentKeyColumn', 'maxRequests', 'incrementalField', 'incrementalInitial', 'concurrency', 'checkpoint', 'onParentError', 'responseMetadata', 'rawResponseDestination', 'httpProxy', 'httpUserAgent', 'httpConnectTimeoutSecs', 'httpReadTimeoutSecs'],
    },
    'src.discord': {
        'kind': 'source',
        'summary': 'Discord REST. Bot token in Authorization header (prefix `Bot `). No native pagination on most endpoints; use `?limit=N&before=ID` patterns. responsePath empty (responses are top-level arrays). Base URL https://discord.com/api/v10.',
        'params': ['url', 'method', 'body', 'headers', 'connectionRef', 'transportRef', 'authType', 'authToken', 'authHeader', 'tokenUrl', 'clientId', 'clientSecret', 'clientAuth', 'scope', 'responsePath', 'jsonPath', 'paginationType', 'cursorNextPath', 'cursorParam', 'offsetParam', 'pageSize', 'totalCountPath', 'pageParam', 'startPage', 'maxPages', 'urlTemplate', 'parentKeyColumn', 'maxRequests', 'incrementalField', 'incrementalInitial', 'concurrency', 'checkpoint', 'onParentError', 'responseMetadata', 'rawResponseDestination', 'httpProxy', 'httpUserAgent', 'httpConnectTimeoutSecs', 'httpReadTimeoutSecs'],
    },
    'src.duckdb': {
        'kind': 'source',
        'summary': 'Read a table from a DuckDB file',
        'params': ['database', 'tableName', 'schema', 'sql'],
    },
    'src.ducklake': {
        'kind': 'source',
        'summary': 'Read tables from a DuckLake catalog (DuckDB native lakehouse)',
        'params': ['path', 'dataPath', 'metadataSchema', 'attachOptions', 'mode', 'schemaName', 'tableName', 'sql', 'asOfVersion', 'asOfTimestamp'],
    },
    'src.ducklake.changes': {
        'kind': 'source',
        'summary': 'Change-data-feed source: reads table_changes() since the last consumed snapshot (saved in workspace state), emitting row-level insert / delete / update_preimage / update_postimage with a change_type column. True incremental CDC for DuckLake-managed tables.',
        'params': ['path', 'dataPath', 'metadataSchema', 'attachOptions', 'schema', 'table', 'insertsOnly', 'initialSnapshot'],
    },
    'src.ducklake.diff': {
        'kind': 'source',
        'summary': 'Data diff between two snapshots of a DuckLake table: emits the row-level change feed (insert / delete / update_preimage / update_postimage with a change_type column) between a chosen From and To snapshot. Pick snapshots with Browse; wire into a validator to assert expected changes in CI.',
        'params': ['path', 'dataPath', 'metadataSchema', 'attachOptions', 'schema', 'table', 'fromVersion', 'toVersion'],
    },
    'src.ducklake.maintain': {
        'kind': 'source',
        'summary': 'Run one of the maintenance operations DuckLake itself provides and emit what it did as ordinary rows. Compact small files, rewrite files heavy with deletes, expire snapshots, clean up files an expired snapshot released, delete orphaned files, flush inlined data, or read per-table storage statisti...',
        'params': ['path', 'dataPath', 'metadataSchema', 'attachOptions', 'operation', 'dryRun', 'schemaName', 'tableName', 'olderThan', 'versions', 'cleanupAll', 'minFileSize', 'maxFileSize', 'maxCompactedFiles', 'deleteThreshold'],
    },
    'src.dynamodb': {
        'kind': 'source',
        'summary': 'Scan a DynamoDB table via direct HTTP + AWS SigV4 signing (no aws-sdk-rust dep). Auto-unwraps the typed-attribute response shape ({S: x}, {N: 5}, {BOOL: t}, {L: [...]}, {M: {...}}) into plain JSON. Pagination follows LastEvaluatedKey. Props: region, accessKeyId, secretAccessKey, sessionToken (opt...',
        'params': ['region', 'tableName', 'accessKeyId', 'secretAccessKey', 'sessionToken', 'limitPerPage', 'maxPages'],
    },
    'src.elastic': {
        'kind': 'source',
        'summary': 'Read docs from an Elasticsearch index via the _search API. from+size pagination (up to 10000 rows by default); ApiKey auth.',
        'params': ['endpoint', 'index', 'apiKey', 'query', 'size', 'maxPages'],
    },
    'src.email': {
        'kind': 'source',
        'summary': 'Fetch the N most recent messages from an IMAP mailbox. TLS via rustls (default port 993). Basic auth (user/password). Each message becomes a row {uid, from, to, subject, date, body_text}. OAuth (gmail / o365) is on the roadmap.',
        'params': ['host', 'port', 'user', 'password', 'mailbox', 'maxMessages'],
    },
    'src.excel': {
        'kind': 'source',
        'summary': 'Read .xlsx via the DuckDB excel extension',
        'params': ['path', 'encoding', 'glob', 'sheet', 'range'],
    },
    'src.filelist': {
        'kind': 'source',
        'summary': 'One row per file in a directory - file (full path) and filename - so a pipeline can iterate a folder. Set a glob pattern and optionally recurse. Pair it with ForEach to process every file.',
        'params': [],
    },
    'src.fixedwidth': {
        'kind': 'source',
        'summary': 'Read positional / fixed-width text files (mainframe / banking exports). Form provides a columns array - {name, start (1-based), width}; engine builds SUBSTR projections. Trailing whitespace stripped by default.',
        'params': ['path', 'encoding', 'glob', 'columnWidths', 'trim'],
    },
    'src.ftp': {
        'kind': 'source',
        'summary': 'List + download files from an FTP server via the pure-Rust suppaftp client. Glob pattern filter (`*`, `?`); each file becomes one row {filename, size, modified, content_b64}. Use DuckDB `from_base64(content_b64)` downstream for the raw bytes. SFTP is a separate protocol and a separate component.',
        'params': ['protocol', 'host', 'port', 'user', 'password', 'privateKeyPath', 'keyPassphrase', 'hostFingerprint', 'directory', 'pattern', 'maxFiles'],
    },
    'src.gcs': {
        'kind': 'source',
        'summary': 'Read via DuckDB httpfs',
        'params': ['bucket', 'key', 'region', 'glob', 'accessKey', 'secretKey', 'sessionToken', 'connectionRef', 'endpoint', 'urlStyle', 'useSsl', 'format', 'hasHeader', 'delimiter', 'quoteChar', 'encoding', 'skipLines', 'nullValue', 'nullPadding', 'ignoreErrors', 'readOptions', 'recordsPath', 'flatten', 'keepParentNames'],
    },
    'src.gdb': {
        'kind': 'source',
        'summary': 'Read a feature class (layer) from an Esri File Geodatabase via the spatial extension (ST_Read with layer=)',
        'params': ['path', 'layer'],
    },
    'src.git': {
        'kind': 'source',
        'summary': 'Read commit log or file tree from a local git working copy. Shells out to the system `git` CLI - no extra Rust dep. mode=log emits {hash, short_hash, author_name, author_email, date, subject}; mode=files emits {mode, type, hash, size, path}.',
        'params': ['repo', 'mode', 'revision', 'pathFilter', 'maxRows'],
    },
    'src.github': {
        'kind': 'source',
        'summary': 'GitHub REST. Bearer Personal Access Token. Link header pagination (paginationType `link`). Accept: application/vnd.github+json header recommended; defaults to https://api.github.com.',
        'params': ['url', 'method', 'body', 'headers', 'connectionRef', 'transportRef', 'authType', 'authToken', 'authHeader', 'tokenUrl', 'clientId', 'clientSecret', 'clientAuth', 'scope', 'responsePath', 'jsonPath', 'paginationType', 'cursorNextPath', 'cursorParam', 'offsetParam', 'pageSize', 'totalCountPath', 'pageParam', 'startPage', 'maxPages', 'urlTemplate', 'parentKeyColumn', 'maxRequests', 'incrementalField', 'incrementalInitial', 'concurrency', 'checkpoint', 'onParentError', 'responseMetadata', 'rawResponseDestination', 'httpProxy', 'httpUserAgent', 'httpConnectTimeoutSecs', 'httpReadTimeoutSecs'],
    },
    'src.gitlab': {
        'kind': 'source',
        'summary': 'GitLab REST. Bearer Personal Access Token. Link header pagination (paginationType `link`). Base URL https://gitlab.com/api/v4 (or self-hosted).',
        'params': ['url', 'method', 'body', 'headers', 'connectionRef', 'transportRef', 'authType', 'authToken', 'authHeader', 'tokenUrl', 'clientId', 'clientSecret', 'clientAuth', 'scope', 'responsePath', 'jsonPath', 'paginationType', 'cursorNextPath', 'cursorParam', 'offsetParam', 'pageSize', 'totalCountPath', 'pageParam', 'startPage', 'maxPages', 'urlTemplate', 'parentKeyColumn', 'maxRequests', 'incrementalField', 'incrementalInitial', 'concurrency', 'checkpoint', 'onParentError', 'responseMetadata', 'rawResponseDestination', 'httpProxy', 'httpUserAgent', 'httpConnectTimeoutSecs', 'httpReadTimeoutSecs'],
    },
    'src.gizmosql': {
        'kind': 'source',
        'summary': 'Query a GizmoSQL (Arrow Flight SQL) server via a clean-room pure-Rust Flight SQL client - no ADBC driver or JDBC needed. Basic-auth handshake then streams Arrow back for fast loads; TLS optional.',
        'params': ['host', 'port', 'username', 'password', 'tls', 'tlsSkipVerify', 'query'],
    },
    'src.graphql': {
        'kind': 'source',
        'summary': 'POST a GraphQL query to an endpoint and walk the response data path. Rides snk.rest/src.rest infrastructure; auth via Bearer / API-Key.',
        'params': ['url', 'query', 'variables', 'headers', 'connectionRef', 'transportRef', 'authType', 'authToken', 'authHeader', 'tokenUrl', 'clientId', 'clientSecret', 'clientAuth', 'scope', 'responsePath', 'jsonPath', 'paginationType', 'cursorNextPath', 'cursorParam', 'offsetParam', 'pageSize', 'totalCountPath', 'pageParam', 'startPage', 'maxPages', 'urlTemplate', 'parentKeyColumn', 'maxRequests', 'incrementalField', 'incrementalInitial', 'concurrency', 'checkpoint', 'onParentError', 'responseMetadata', 'rawResponseDestination', 'httpProxy', 'httpUserAgent', 'httpConnectTimeoutSecs', 'httpReadTimeoutSecs'],
    },
    'src.html': {
        'kind': 'source',
        'summary': 'Rows out of an HTML page, by CSS selector. Point it at a local file or an http(s) URL, give a row selector, and either name a column per sub-selector (with an optional attribute, so a link href or a data- value is readable) or leave the columns empty and let a table become a table: the th cells n...',
        'params': ['path', 'rowSelector', 'columns', 'transportRef', 'authType', 'authToken', 'headers', 'uriColumn', 'carryColumns', 'shaColumn', 'onError', 'accessKey', 'secretKey', 'sessionToken', 'region', 'endpoint', 'urlStyle', 'useSsl', 'user', 'password', 'privateKey', 'keyPassphrase', 'hostFingerprint', 'nextPageSelector', 'nextPageAttribute', 'maxPages', 'rawResponseDestination', 'cacheOutput', 'httpProxy', 'httpUserAgent', 'httpConnectTimeoutSecs', 'httpReadTimeoutSecs'],
    },
    'src.http': {
        'kind': 'source',
        'summary': 'Read CSV / Parquet / JSON from any HTTP(S) URL via httpfs',
        'params': ['url', 'method', 'body', 'headers', 'connectionRef', 'transportRef', 'authType', 'authToken', 'authHeader', 'tokenUrl', 'clientId', 'clientSecret', 'clientAuth', 'scope', 'responsePath', 'jsonPath', 'paginationType', 'cursorNextPath', 'cursorParam', 'offsetParam', 'pageSize', 'totalCountPath', 'pageParam', 'startPage', 'maxPages', 'urlTemplate', 'parentKeyColumn', 'maxRequests', 'concurrency', 'checkpoint', 'onParentError', 'responseMetadata', 'rawResponseDestination', 'hasHeader', 'delimiter', 'quoteChar', 'encoding', 'skipLines', 'nullValue', 'nullPadding', 'ignoreErrors', 'readOptions', 'recordsPath', 'flatten', 'keepParentNames'],
    },
    'src.hubspot': {
        'kind': 'source',
        'summary': 'HubSpot REST. Bearer auth via a Private App access token. Cursor pagination on `paging.next.after` (cursorNextPath /paging/next/after, cursorParam `after`). responsePath /results.',
        'params': ['url', 'method', 'body', 'headers', 'connectionRef', 'transportRef', 'authType', 'authToken', 'authHeader', 'tokenUrl', 'clientId', 'clientSecret', 'clientAuth', 'scope', 'responsePath', 'jsonPath', 'paginationType', 'cursorNextPath', 'cursorParam', 'offsetParam', 'pageSize', 'totalCountPath', 'pageParam', 'startPage', 'maxPages', 'urlTemplate', 'parentKeyColumn', 'maxRequests', 'incrementalField', 'incrementalInitial', 'concurrency', 'checkpoint', 'onParentError', 'responseMetadata', 'rawResponseDestination', 'httpProxy', 'httpUserAgent', 'httpConnectTimeoutSecs', 'httpReadTimeoutSecs'],
    },
    'src.huggingface': {
        'kind': 'source',
        'summary': 'Read a Hugging Face Hub dataset directly via DuckDB hf:// (httpfs). Give the repo id and a file/glob; CSV / JSON / Parquet auto-detected. Token for private or gated datasets.',
        'params': ['repo', 'path', 'revision', 'token'],
    },
    'src.iceberg': {
        'kind': 'source',
        'summary': 'Read Iceberg tables via DuckDB iceberg_scan',
        'params': ['path'],
    },
    'src.inline': {
        'kind': 'source',
        'summary': 'Rows you write here rather than read from anywhere: a control row, an audit stamp, a fixed lookup. Give each column a name and a value; rowCount repeats the row. Every other source names an external system, so this was previously a throwaway file.',
        'params': [],
    },
    'src.intercom': {
        'kind': 'source',
        'summary': 'Intercom REST. Bearer auth. Cursor pagination via `pages.next.starting_after` + `starting_after` param. responsePath /data.',
        'params': ['url', 'method', 'body', 'headers', 'connectionRef', 'transportRef', 'authType', 'authToken', 'authHeader', 'tokenUrl', 'clientId', 'clientSecret', 'clientAuth', 'scope', 'responsePath', 'jsonPath', 'paginationType', 'cursorNextPath', 'cursorParam', 'offsetParam', 'pageSize', 'totalCountPath', 'pageParam', 'startPage', 'maxPages', 'urlTemplate', 'parentKeyColumn', 'maxRequests', 'incrementalField', 'incrementalInitial', 'concurrency', 'checkpoint', 'onParentError', 'responseMetadata', 'rawResponseDestination', 'httpProxy', 'httpUserAgent', 'httpConnectTimeoutSecs', 'httpReadTimeoutSecs'],
    },
    'src.jira': {
        'kind': 'source',
        'summary': 'Jira Cloud REST. Basic auth (email + API token). Offset pagination on `startAt` + `maxResults`. responsePath /issues for /search.',
        'params': ['url', 'method', 'body', 'headers', 'connectionRef', 'transportRef', 'authType', 'authToken', 'authHeader', 'tokenUrl', 'clientId', 'clientSecret', 'clientAuth', 'scope', 'responsePath', 'jsonPath', 'paginationType', 'cursorNextPath', 'cursorParam', 'offsetParam', 'pageSize', 'totalCountPath', 'pageParam', 'startPage', 'maxPages', 'urlTemplate', 'parentKeyColumn', 'maxRequests', 'incrementalField', 'incrementalInitial', 'concurrency', 'checkpoint', 'onParentError', 'responseMetadata', 'rawResponseDestination', 'httpProxy', 'httpUserAgent', 'httpConnectTimeoutSecs', 'httpReadTimeoutSecs'],
    },
    'src.json': {
        'kind': 'source',
        'summary': 'Read JSON files',
        'params': ['path', 'format', 'flatten', 'keepParentNames', 'recordsPath', 'ignoreErrors'],
    },
    'src.jsonl': {
        'kind': 'source',
        'summary': 'Read newline-delimited JSON',
        'params': ['path', 'encoding', 'glob', 'format', 'flatten', 'keepParentNames', 'sampleSize', 'recordsPath'],
    },
    'src.kafka': {
        'kind': 'source',
        'summary': 'Batch-consume up to maxRecords messages from a single partition via the pure-Rust `rskafka` driver. Emits {offset, key, value, timestamp_ms} rows. startOffset negative = read from earliest available; positive = read from that offset. Batch ETL semantics - continuous streaming is on the roadmap.',
        'params': ['brokers', 'topic', 'offset', 'trackOffset', 'security', 'saslMechanism', 'saslUsername', 'saslPassword', 'format', 'schemaRegistryUrl'],
    },
    'src.kinesis': {
        'kind': 'source',
        'summary': 'Single-shard Kinesis read via direct HTTP + AWS SigV4 (no AWS SDK). Walks ListShards -> GetShardIterator -> GetRecords. Props: region, accessKeyId, secretAccessKey, sessionToken (optional STS), streamName, shardIndex (default 0), iteratorType (TRIM_HORIZON or LATEST), maxRecords. Records with JSO...',
        'params': ['region', 'streamName', 'accessKeyId', 'secretAccessKey', 'sessionToken', 'shardIndex', 'iteratorType', 'maxRecords'],
    },
    'src.lancedb': {
        'kind': 'source',
        'summary': 'Read a Lance table (local dir, LanceDB Cloud db://, or s3:// / gs:// / az:// object store) via the bundled duckle-lance sidecar.',
        'params': ['uri', 'table', 'limit', 'apiKey', 'region'],
    },
    'src.linear': {
        'kind': 'source',
        'summary': 'Linear GraphQL. Rides src.graphql; auth via API key in Authorization header. responsePath walks /data.<query>.<edges> or similar.',
        'params': ['url', 'query', 'variables', 'headers', 'connectionRef', 'transportRef', 'authType', 'authToken', 'authHeader', 'tokenUrl', 'clientId', 'clientSecret', 'clientAuth', 'scope', 'responsePath', 'jsonPath', 'paginationType', 'cursorNextPath', 'cursorParam', 'offsetParam', 'pageSize', 'totalCountPath', 'pageParam', 'startPage', 'maxPages', 'urlTemplate', 'parentKeyColumn', 'maxRequests', 'incrementalField', 'incrementalInitial', 'concurrency', 'checkpoint', 'onParentError', 'responseMetadata', 'rawResponseDestination', 'httpProxy', 'httpUserAgent', 'httpConnectTimeoutSecs', 'httpReadTimeoutSecs'],
    },
    'src.mailchimp': {
        'kind': 'source',
        'summary': 'Mailchimp REST. Bearer API key (the key has a region suffix - the URL is https://{region}.api.mailchimp.com/3.0). Offset pagination via `offset` + `count`. responsePath /lists (or /campaigns / etc).',
        'params': ['url', 'method', 'body', 'headers', 'connectionRef', 'transportRef', 'authType', 'authToken', 'authHeader', 'tokenUrl', 'clientId', 'clientSecret', 'clientAuth', 'scope', 'responsePath', 'jsonPath', 'paginationType', 'cursorNextPath', 'cursorParam', 'offsetParam', 'pageSize', 'totalCountPath', 'pageParam', 'startPage', 'maxPages', 'urlTemplate', 'parentKeyColumn', 'maxRequests', 'incrementalField', 'incrementalInitial', 'concurrency', 'checkpoint', 'onParentError', 'responseMetadata', 'rawResponseDestination', 'httpProxy', 'httpUserAgent', 'httpConnectTimeoutSecs', 'httpReadTimeoutSecs'],
    },
    'src.mariadb': {
        'kind': 'source',
        'summary': 'Read from MariaDB via the DuckDB mysql extension',
        'params': ['connectionRef', 'host', 'port', 'database', 'username', 'password', 'mode', 'schemaName', 'tableName', 'sql', 'pushdown', 'readOnly', 'connString'],
        'unverified': ['fetchSize'],
    },
    'src.milvus': {
        'kind': 'source',
        'summary': 'Query Milvus via POST /v1/vector/query. Offset pagination on `offset` + `limit`; emits each `data[]` element as a row. Provide a filter expression (default `id > 0`) and optional outputFields. apiKey via Bearer.',
        'params': ['endpoint', 'apiKey', 'collection', 'connectionRef', 'topK', 'filter'],
        'unverified': ['queryMode', 'queryText'],
    },
    'src.minio': {
        'kind': 'source',
        'summary': 'Read via S3-compatible endpoint',
        'params': ['bucket', 'key', 'region', 'glob', 'accessKey', 'secretKey', 'sessionToken', 'connectionRef', 'endpoint', 'urlStyle', 'useSsl', 'format', 'hasHeader', 'delimiter', 'quoteChar', 'encoding', 'skipLines', 'nullValue', 'nullPadding', 'ignoreErrors', 'readOptions', 'recordsPath', 'flatten', 'keepParentNames'],
    },
    'src.model': {
        'kind': 'source',
        'summary': 'Read a registered model card back as one row: name, version, the artifact URI the training script wrote, and whatever metrics and hashes it recorded. Address it as name@version, or name@latest to follow the pointer that moves on every successful retrain, so a scoring pipeline stays unedited. The ...',
        'params': ['model', 'path'],
    },
    'src.monday': {
        'kind': 'source',
        'summary': 'Monday.com GraphQL. Rides src.graphql; auth via Bearer token in Authorization header. POST a GraphQL query as `body`; responsePath /data.<query_name>. Base URL https://api.monday.com/v2.',
        'params': ['url', 'query', 'variables', 'headers', 'connectionRef', 'transportRef', 'authType', 'authToken', 'authHeader', 'tokenUrl', 'clientId', 'clientSecret', 'clientAuth', 'scope', 'responsePath', 'jsonPath', 'paginationType', 'cursorNextPath', 'cursorParam', 'offsetParam', 'pageSize', 'totalCountPath', 'pageParam', 'startPage', 'maxPages', 'urlTemplate', 'parentKeyColumn', 'maxRequests', 'incrementalField', 'incrementalInitial', 'concurrency', 'checkpoint', 'onParentError', 'responseMetadata', 'rawResponseDestination', 'httpProxy', 'httpUserAgent', 'httpConnectTimeoutSecs', 'httpReadTimeoutSecs'],
    },
    'src.mongodb': {
        'kind': 'source',
        'summary': 'Read documents from a MongoDB collection via the official Rust driver (find with optional filter / projection / limit). Auth via mongodb:// connection string.',
        'params': ['uri', 'database', 'collection', 'filter', 'projection', 'limit', 'pipeline'],
    },
    'src.motherduck': {
        'kind': 'source',
        'summary': 'Read from MotherDuck via ATTACH md:',
        'params': ['database', 'token', 'mode', 'schemaName', 'tableName', 'sql'],
    },
    'src.mysql': {
        'kind': 'source',
        'summary': 'Read from MySQL via the DuckDB mysql extension',
        'params': ['connectionRef', 'host', 'port', 'database', 'username', 'password', 'mode', 'schemaName', 'tableName', 'sql', 'pushdown', 'readOnly', 'connString'],
        'unverified': ['fetchSize'],
    },
    'src.nats': {
        'kind': 'source',
        'summary': 'Subscribe-with-timeout collector via the pure-Rust `async-nats` driver. Drains up to maxRecords messages from subject within timeoutMs wall-clock. Emits {subject, payload} rows. Batch ETL semantics - continuous streaming is on the roadmap.',
        'params': ['urls', 'subject', 'maxRecords', 'timeoutMs'],
    },
    'src.neo4j': {
        'kind': 'source',
        'summary': 'Run Cypher against Neo4j over the HTTP Query API (/db/{database}/query/v2) - works with a self-hosted server and with Aura, and needs no Bolt driver. Basic auth; optional Cypher $parameters. Node and relationship values keep their properties as structs.',
        'params': ['endpoint', 'database', 'user', 'password', 'cypher', 'parameters'],
    },
    'src.notion': {
        'kind': 'source',
        'summary': 'Notion REST. Bearer integration token + Notion-Version header. Cursor pagination on `next_cursor` (cursorNextPath /next_cursor, cursorParam `start_cursor`). responsePath /results.',
        'params': ['url', 'method', 'body', 'headers', 'connectionRef', 'transportRef', 'authType', 'authToken', 'authHeader', 'tokenUrl', 'clientId', 'clientSecret', 'clientAuth', 'scope', 'responsePath', 'jsonPath', 'paginationType', 'cursorNextPath', 'cursorParam', 'offsetParam', 'pageSize', 'totalCountPath', 'pageParam', 'startPage', 'maxPages', 'urlTemplate', 'parentKeyColumn', 'maxRequests', 'incrementalField', 'incrementalInitial', 'concurrency', 'checkpoint', 'onParentError', 'responseMetadata', 'rawResponseDestination', 'httpProxy', 'httpUserAgent', 'httpConnectTimeoutSecs', 'httpReadTimeoutSecs'],
    },
    'src.odata': {
        'kind': 'source',
        'summary': 'OData v4 source - thin alias over src.rest. Defaults: responsePath /value, pagination follows @odata.nextLink as a complete URL. Set authType (basic / bearer / apikey) on the form. Works with SAP, D365, Microsoft Graph, any OData v4 endpoint.',
        'params': ['url', 'method', 'body', 'headers', 'connectionRef', 'transportRef', 'authType', 'authToken', 'authHeader', 'tokenUrl', 'clientId', 'clientSecret', 'clientAuth', 'scope', 'responsePath', 'jsonPath', 'paginationType', 'cursorNextPath', 'cursorParam', 'offsetParam', 'pageSize', 'totalCountPath', 'pageParam', 'startPage', 'maxPages', 'urlTemplate', 'parentKeyColumn', 'maxRequests', 'incrementalField', 'incrementalInitial', 'concurrency', 'checkpoint', 'onParentError', 'responseMetadata', 'rawResponseDestination', 'httpProxy', 'httpUserAgent', 'httpConnectTimeoutSecs', 'httpReadTimeoutSecs'],
    },
    'src.opensearch': {
        'kind': 'source',
        'summary': 'Read docs from an OpenSearch index via the _search API. Same wire as Elasticsearch; same ApiKey auth.',
        'params': ['endpoint', 'index', 'apiKey', 'query', 'size', 'maxPages'],
    },
    'src.oracle': {
        'kind': 'source',
        'summary': "Read Oracle via the official `oracle` Rust crate (ODPI-C). Built into the shipped binary - users need Oracle Instant Client (libclntsh.{so,dll,dylib}) on the library path at RUNTIME; the executor surfaces a clear OCI loader error if it's missing. SQL auth via user / password; EZ Connect string fo...",
        'params': ['connect', 'user', 'password', 'schema', 'tableName', 'query', 'parallelColumn', 'parallelDegree'],
        'unverified': ['oracleRuntimeNote'],
    },
    'src.parquet': {
        'kind': 'source',
        'summary': 'Read columnar Parquet files',
        'params': ['path', 'columns'],
        'unverified': ['rowGroupRange'],
    },
    'src.pdf': {
        'kind': 'source',
        'summary': 'One row per PAGE of a PDF: document_id, page_number, text, has_text_layer, width, height and the document metadata. Point it at a file or a folder. Reads the text layer a document already carries, so filings, accounts and invoices become a table you can filter, join and hand to a Python or AI sta...',
        'params': ['path', 'recursive', 'uriColumn', 'carryColumns', 'shaColumn', 'onError', 'accessKey', 'secretKey', 'sessionToken', 'region', 'endpoint', 'urlStyle', 'useSsl', 'headers', 'user', 'password', 'privateKey', 'keyPassphrase', 'hostFingerprint', 'cacheOutput'],
    },
    'src.pgvector': {
        'kind': 'source',
        'summary': 'Read embeddings + metadata via DuckDB postgres ATTACH (server must have CREATE EXTENSION vector)',
        'params': ['connectionRef', 'host', 'port', 'database', 'username', 'password', 'sslmode', 'sslrootcert', 'sslcert', 'sslkey', 'connectTimeout', 'options', 'connParams', 'schemaName', 'tableName'],
    },
    'src.pinecone': {
        'kind': 'source',
        'summary': 'Fetch or similarity-search vectors (Pinecone has no list-all-vectors endpoint; the proper shape is a query node, on the roadmap)',
        'params': ['endpoint', 'apiKey', 'collection', 'connectionRef', 'topK', 'filter'],
        'unverified': ['queryMode', 'queryText'],
    },
    'src.pipedrive': {
        'kind': 'source',
        'summary': 'Pipedrive REST. URL ?api_token=... or Bearer auth. Cursor pagination on `additional_data.pagination.next_start` (start parameter). responsePath /data.',
        'params': ['url', 'method', 'body', 'headers', 'connectionRef', 'transportRef', 'authType', 'authToken', 'authHeader', 'tokenUrl', 'clientId', 'clientSecret', 'clientAuth', 'scope', 'responsePath', 'jsonPath', 'paginationType', 'cursorNextPath', 'cursorParam', 'offsetParam', 'pageSize', 'totalCountPath', 'pageParam', 'startPage', 'maxPages', 'urlTemplate', 'parentKeyColumn', 'maxRequests', 'incrementalField', 'incrementalInitial', 'concurrency', 'checkpoint', 'onParentError', 'responseMetadata', 'rawResponseDestination', 'httpProxy', 'httpUserAgent', 'httpConnectTimeoutSecs', 'httpReadTimeoutSecs'],
    },
    'src.pixeltable': {
        'kind': 'source',
        'summary': 'Read a Pixeltable table (#223), the multimodal AI data store. Exchanges through Parquet: Pixeltable exports the table (or a filtered/limited subset) and Duckle ingests it with read_parquet, so no rows cross one at a time. Supports versioned reads via table:N. Needs a Python with pixeltable instal...',
        'params': ['table', 'columns', 'filter', 'limit'],
    },
    'src.postgres': {
        'kind': 'source',
        'summary': 'Read from PostgreSQL via the DuckDB postgres extension',
        'params': ['connectionRef', 'host', 'port', 'database', 'username', 'password', 'sslmode', 'sslrootcert', 'sslcert', 'sslkey', 'connectTimeout', 'options', 'connParams', 'mode', 'schemaName', 'tableName', 'sql', 'pushdown', 'readOnly', 'connString'],
        'unverified': ['fetchSize'],
    },
    'src.pubsub': {
        'kind': 'source',
        'summary': 'Pull messages via the Pub/Sub REST API (POST /v1/projects/{p}/subscriptions/{s}:pull) - sidesteps the gRPC build dependency. Auto-acks the batch. Auth via a pre-fetched OAuth2 Bearer access token (mint with `gcloud auth print-access-token`). Emits {message_id, publish_time, data} rows.',
        'params': ['project', 'subscription', 'accessToken', 'maxMessages'],
    },
    'src.qdrant': {
        'kind': 'source',
        'summary': 'Scroll all points in a Qdrant collection via /collections/{id}/points/scroll. Cursor pagination on `result.next_page_offset`; emits {id, ...payload[, vector]} rows. apiKey via api-key header.',
        'params': ['connectionRef', 'clusterUrl', 'collection', 'apiKey', 'pageSize', 'maxPages', 'withVector'],
    },
    'src.quack': {
        'kind': 'source',
        'summary': 'Read tables from a remote DuckDB instance over the Quack protocol (HTTP on port 9494). Server runs quack_serve(...); client ATTACHes the quack: URL with a token-based SECRET.',
        'params': ['host', 'port', 'token', 'mode', 'schemaName', 'tableName', 'sql'],
    },
    'src.quickbooks': {
        'kind': 'source',
        'summary': "QuickBooks Online REST. Bearer OAuth token; users assemble the query URL (Intuit's API requires SQL-like queries). responsePath /QueryResponse.",
        'params': ['url', 'method', 'body', 'headers', 'connectionRef', 'transportRef', 'authType', 'authToken', 'authHeader', 'tokenUrl', 'clientId', 'clientSecret', 'clientAuth', 'scope', 'responsePath', 'jsonPath', 'paginationType', 'cursorNextPath', 'cursorParam', 'offsetParam', 'pageSize', 'totalCountPath', 'pageParam', 'startPage', 'maxPages', 'urlTemplate', 'parentKeyColumn', 'maxRequests', 'incrementalField', 'incrementalInitial', 'concurrency', 'checkpoint', 'onParentError', 'responseMetadata', 'rawResponseDestination', 'httpProxy', 'httpUserAgent', 'httpConnectTimeoutSecs', 'httpReadTimeoutSecs'],
    },
    'src.qvd': {
        'kind': 'source',
        'summary': 'Qlik QVD files (.qvd) via a clean-room pure-Rust reader (no Qlik runtime). The QVD header carries its own schema; the symbol table + bit-stuffed index are decoded directly. Move QlikView / Qlik Sense extracts into DuckDB, Parquet or any sink.',
        'params': ['path', 'encoding', 'glob'],
    },
    'src.r2': {
        'kind': 'source',
        'summary': 'Read via S3-compatible endpoint',
        'params': ['bucket', 'key', 'region', 'glob', 'accessKey', 'secretKey', 'sessionToken', 'connectionRef', 'endpoint', 'urlStyle', 'useSsl', 'format', 'hasHeader', 'delimiter', 'quoteChar', 'encoding', 'skipLines', 'nullValue', 'nullPadding', 'ignoreErrors', 'readOptions', 'recordsPath', 'flatten', 'keepParentNames'],
    },
    'src.rabbit': {
        'kind': 'source',
        'summary': 'Pull messages from a queue via the pure-Rust `lapin` AMQP 0.9.1 driver. Polls until maxMessages or timeoutMs wall-clock elapses; auto-acks each pulled message. Emits {payload, routing_key, exchange, delivery_tag} rows.',
        'params': ['url', 'queue', 'maxMessages', 'timeoutMs'],
    },
    'src.redis': {
        'kind': 'source',
        'summary': "SCAN keys matching a pattern (default *) and GET each value via the sync `redis` Rust client. Emits {key, value} rows. limit caps the walk so a million-key DB doesn't spin forever.",
        'params': ['connectionString', 'database', 'collection', 'filter', 'projection', 'limit'],
        'unverified': ['queryMode'],
    },
    'src.redpanda': {
        'kind': 'source',
        'summary': 'Same wire protocol as Kafka - rides the rskafka driver. Use src.kafka semantics: batch-consume up to maxRecords from a single partition.',
        'params': ['brokers', 'topic', 'offset', 'trackOffset', 'security', 'saslMechanism', 'saslUsername', 'saslPassword', 'format', 'schemaRegistryUrl'],
    },
    'src.redshift': {
        'kind': 'source',
        'summary': 'Read Redshift via the postgres ATTACH path (Redshift speaks Postgres wire on port 5439)',
        'params': ['connectionRef', 'host', 'port', 'database', 'username', 'password', 'sslmode', 'sslrootcert', 'sslcert', 'sslkey', 'connectTimeout', 'options', 'connParams', 'schemaName', 'tableName', 'query', 'pushdown'],
    },
    'src.rest': {
        'kind': 'source',
        'summary': 'Generic HTTP GET/POST source. Parses JSON response, optionally walks a JSON pointer (responsePath) to find the row array, and follows cursor-style pagination if configured (cursorNextPath + cursorParam).',
        'params': ['url', 'method', 'body', 'headers', 'connectionRef', 'transportRef', 'authType', 'authToken', 'authHeader', 'tokenUrl', 'clientId', 'clientSecret', 'clientAuth', 'scope', 'responsePath', 'jsonPath', 'paginationType', 'cursorNextPath', 'cursorParam', 'offsetParam', 'pageSize', 'totalCountPath', 'pageParam', 'startPage', 'maxPages', 'urlTemplate', 'parentKeyColumn', 'maxRequests', 'incrementalField', 'incrementalInitial', 'concurrency', 'checkpoint', 'onParentError', 'responseMetadata', 'rawResponseDestination', 'httpProxy', 'httpUserAgent', 'httpConnectTimeoutSecs', 'httpReadTimeoutSecs'],
    },
    'src.s3': {
        'kind': 'source',
        'summary': 'Read via DuckDB httpfs',
        'params': ['path', 'connectionRef', 'format', 'accessKey', 'secretKey', 'region', 'hasHeader', 'delimiter', 'quoteChar', 'encoding', 'skipLines', 'nullValue', 'nullPadding', 'ignoreErrors', 'readOptions', 'recordsPath', 'flatten', 'keepParentNames'],
    },
    'src.salesforce': {
        'kind': 'source',
        'summary': 'Salesforce REST. Rides the generic src.rest path with a Bearer token or OAuth 2.0 client-credentials (a fresh token minted per run from a connected app); users typically point url at https://{instance}.my.salesforce.com/services/data/v60.0/query/?q=SELECT+... and walk responsePath /records.',
        'params': ['url', 'method', 'body', 'headers', 'connectionRef', 'transportRef', 'authType', 'authToken', 'authHeader', 'loginUrl', 'clientId', 'clientSecret', 'responsePath', 'jsonPath', 'paginationType', 'cursorNextPath', 'cursorParam', 'offsetParam', 'pageSize', 'totalCountPath', 'pageParam', 'startPage', 'maxPages', 'urlTemplate', 'parentKeyColumn', 'maxRequests', 'incrementalField', 'incrementalInitial', 'concurrency', 'checkpoint', 'onParentError', 'responseMetadata', 'rawResponseDestination', 'httpProxy', 'httpUserAgent', 'httpConnectTimeoutSecs', 'httpReadTimeoutSecs'],
    },
    'src.salesforce.bulk': {
        'kind': 'source',
        'summary': 'Salesforce Bulk API 2.0 query source for migration-scale reads: a SOQL query runs as an async query job (query / queryAll incl. deleted+archived), the paged CSV result sets stream to disk via Sforce-Locator, and DuckDB reads them out-of-core - a multi-GB result never lands in memory. Same auth as...',
        'params': ['connectionRef', 'authMode', 'instanceUrl', 'accessToken', 'loginUrl', 'clientId', 'clientSecret', 'apiVersion', 'query', 'operation', 'maxRecords', 'pollIntervalSecs', 'timeoutSecs'],
    },
    'src.sap': {
        'kind': 'source',
        'summary': 'SAP S/4HANA & ECC source over OData - covers OData services and CDS views published as OData (@OData.publish). Native HTTP, no SAP GUI or SDK. Set odataVersion (v2 classic Gateway = /d/results with __next paging; v4 RAP = /value with @odata.nextLink), sapClient (mandate, appended as sap-client=NN...',
        'params': ['odataVersion', 'sapClient', 'url', 'method', 'body', 'headers', 'connectionRef', 'transportRef', 'authType', 'authToken', 'authHeader', 'tokenUrl', 'clientId', 'clientSecret', 'clientAuth', 'scope', 'responsePath', 'jsonPath', 'paginationType', 'cursorNextPath', 'cursorParam', 'offsetParam', 'pageSize', 'totalCountPath', 'pageParam', 'startPage', 'maxPages', 'urlTemplate', 'parentKeyColumn', 'maxRequests', 'incrementalField', 'incrementalInitial', 'concurrency', 'checkpoint', 'onParentError', 'responseMetadata', 'rawResponseDestination', 'httpProxy', 'httpUserAgent', 'httpConnectTimeoutSecs', 'httpReadTimeoutSecs'],
    },
    'src.sap.rfc': {
        'kind': 'source',
        'summary': 'Call an RFC-enabled function module / BAPI exposed as a SOAP web service (SOAMANAGER, or the generic /sap/bc/soap/rfc endpoint). Native HTTP + XML, no proprietary SAP NW RFC SDK. Set url to the service endpoint, body to the SOAP envelope, responsePath to the element-name walk to the result table,...',
        'params': ['url', 'method', 'body', 'headers', 'connectionRef', 'transportRef', 'authType', 'authToken', 'authHeader', 'tokenUrl', 'clientId', 'clientSecret', 'clientAuth', 'scope', 'responsePath', 'jsonPath', 'paginationType', 'cursorNextPath', 'cursorParam', 'offsetParam', 'pageSize', 'totalCountPath', 'pageParam', 'startPage', 'maxPages', 'urlTemplate', 'parentKeyColumn', 'maxRequests', 'incrementalField', 'incrementalInitial', 'concurrency', 'checkpoint', 'onParentError', 'responseMetadata', 'rawResponseDestination'],
    },
    'src.scylla': {
        'kind': 'source',
        'summary': 'Read CQL via the scylla driver. Same wire as src.cassandra.',
        'params': ['contactPoints', 'user', 'password', 'keyspace', 'tableName', 'query'],
    },
    'src.segment': {
        'kind': 'source',
        'summary': 'Segment Public API. Bearer access token. Cursor pagination via `pagination.next` + `pagination[cursor]` param. responsePath /data.',
        'params': ['url', 'method', 'body', 'headers', 'connectionRef', 'transportRef', 'authType', 'authToken', 'authHeader', 'tokenUrl', 'clientId', 'clientSecret', 'clientAuth', 'scope', 'responsePath', 'jsonPath', 'paginationType', 'cursorNextPath', 'cursorParam', 'offsetParam', 'pageSize', 'totalCountPath', 'pageParam', 'startPage', 'maxPages', 'urlTemplate', 'parentKeyColumn', 'maxRequests', 'incrementalField', 'incrementalInitial', 'concurrency', 'checkpoint', 'onParentError', 'responseMetadata', 'rawResponseDestination', 'httpProxy', 'httpUserAgent', 'httpConnectTimeoutSecs', 'httpReadTimeoutSecs'],
    },
    'src.sendgrid': {
        'kind': 'source',
        'summary': 'SendGrid REST. Bearer API key. Offset pagination via `offset` + `limit`. responsePath /result for /v3/marketing/* endpoints.',
        'params': ['url', 'method', 'body', 'headers', 'connectionRef', 'transportRef', 'authType', 'authToken', 'authHeader', 'tokenUrl', 'clientId', 'clientSecret', 'clientAuth', 'scope', 'responsePath', 'jsonPath', 'paginationType', 'cursorNextPath', 'cursorParam', 'offsetParam', 'pageSize', 'totalCountPath', 'pageParam', 'startPage', 'maxPages', 'urlTemplate', 'parentKeyColumn', 'maxRequests', 'incrementalField', 'incrementalInitial', 'concurrency', 'checkpoint', 'onParentError', 'responseMetadata', 'rawResponseDestination', 'httpProxy', 'httpUserAgent', 'httpConnectTimeoutSecs', 'httpReadTimeoutSecs'],
    },
    'src.shopify': {
        'kind': 'source',
        'summary': 'Shopify Admin API. Bearer auth via X-Shopify-Access-Token. Link header pagination supported by recent Admin API endpoints. responsePath depends on resource (e.g. /products).',
        'params': ['url', 'method', 'body', 'headers', 'connectionRef', 'transportRef', 'authType', 'authToken', 'authHeader', 'tokenUrl', 'clientId', 'clientSecret', 'clientAuth', 'scope', 'responsePath', 'jsonPath', 'paginationType', 'cursorNextPath', 'cursorParam', 'offsetParam', 'pageSize', 'totalCountPath', 'pageParam', 'startPage', 'maxPages', 'urlTemplate', 'parentKeyColumn', 'maxRequests', 'incrementalField', 'incrementalInitial', 'concurrency', 'checkpoint', 'onParentError', 'responseMetadata', 'rawResponseDestination', 'httpProxy', 'httpUserAgent', 'httpConnectTimeoutSecs', 'httpReadTimeoutSecs'],
    },
    'src.slack': {
        'kind': 'source',
        'summary': 'Slack Web API. Bearer Bot User OAuth Token (xoxb-...). Cursor pagination via `response_metadata.next_cursor` + `cursor` param. responsePath depends on endpoint (e.g. /messages for conversations.history). Base URL https://slack.com/api.',
        'params': ['url', 'method', 'body', 'headers', 'connectionRef', 'transportRef', 'authType', 'authToken', 'authHeader', 'tokenUrl', 'clientId', 'clientSecret', 'clientAuth', 'scope', 'responsePath', 'jsonPath', 'paginationType', 'cursorNextPath', 'cursorParam', 'offsetParam', 'pageSize', 'totalCountPath', 'pageParam', 'startPage', 'maxPages', 'urlTemplate', 'parentKeyColumn', 'maxRequests', 'incrementalField', 'incrementalInitial', 'concurrency', 'checkpoint', 'onParentError', 'responseMetadata', 'rawResponseDestination', 'httpProxy', 'httpUserAgent', 'httpConnectTimeoutSecs', 'httpReadTimeoutSecs'],
    },
    'src.snowflake': {
        'kind': 'source',
        'summary': 'Read Snowflake via the SQL API (/api/v2/statements). Supports PAT and JWT RS256 auth; engine materializes inline result sets as a DuckDB table for downstream stages.',
        'params': ['account', 'authType', 'pat', 'user', 'privateKeyPath', 'warehouse', 'role', 'endpoint', 'database', 'schema', 'tableName', 'query'],
    },
    'src.soap': {
        'kind': 'source',
        'summary': 'SOAP / generic XML-API source. Thin alias over src.rest with defaults: POST, Content-Type text/xml; charset=utf-8, responseFormat=xml. Set responsePath to the element-name walk into the body (e.g. Envelope/Body/GetUsersResponse/Users/User), supply the XML envelope in `body`, optionally add a `soa...',
        'params': ['url', 'method', 'body', 'headers', 'connectionRef', 'transportRef', 'authType', 'authToken', 'authHeader', 'tokenUrl', 'clientId', 'clientSecret', 'clientAuth', 'scope', 'responsePath', 'jsonPath', 'paginationType', 'cursorNextPath', 'cursorParam', 'offsetParam', 'pageSize', 'totalCountPath', 'pageParam', 'startPage', 'maxPages', 'urlTemplate', 'parentKeyColumn', 'maxRequests', 'incrementalField', 'incrementalInitial', 'concurrency', 'checkpoint', 'onParentError', 'responseMetadata', 'rawResponseDestination', 'httpProxy', 'httpUserAgent', 'httpConnectTimeoutSecs', 'httpReadTimeoutSecs'],
    },
    'src.spatial': {
        'kind': 'source',
        'summary': 'Read geospatial files: GeoParquet natively, and GeoJSON / Shapefile / GeoPackage / KML / GPX / GML via the DuckDB spatial extension (ST_Read)',
        'params': ['path', 'encoding', 'glob'],
    },
    'src.spool': {
        'kind': 'source',
        'summary': 'Tail an append-only NDJSON file from where the last SUCCESSFUL run stopped, by byte offset. Pairs with `duckle-runner listen`, which keeps a webhook listener up and appends here - so nothing is lost between pipeline runs, unlike src.webhook which only collects while a run is executing. A failed r...',
        'params': ['path', 'trackOffset', 'maxBytes'],
    },
    'src.sqlite': {
        'kind': 'source',
        'summary': 'Read SQLite tables',
        'params': ['database', 'mode', 'tableName', 'sql'],
    },
    'src.sqlserver': {
        'kind': 'source',
        'summary': 'Read SQL Server via the native TDS protocol (tiberius, pure Rust). SQL auth (user/password); trust_cert option for self-signed dev servers.',
        'params': ['connectionRef', 'host', 'port', 'user', 'password', 'database', 'trustCert', 'encrypt', 'schema', 'tableName', 'query'],
    },
    'src.stripe': {
        'kind': 'source',
        'summary': 'Stripe REST. Bearer auth with the Secret Key (sk_live_... / sk_test_...). Cursor pagination on `data[-1].id` via `starting_after`. responsePath /data.',
        'params': ['url', 'method', 'body', 'headers', 'connectionRef', 'transportRef', 'authType', 'authToken', 'authHeader', 'tokenUrl', 'clientId', 'clientSecret', 'clientAuth', 'scope', 'responsePath', 'jsonPath', 'paginationType', 'cursorNextPath', 'cursorParam', 'offsetParam', 'pageSize', 'totalCountPath', 'pageParam', 'startPage', 'maxPages', 'urlTemplate', 'parentKeyColumn', 'maxRequests', 'incrementalField', 'incrementalInitial', 'concurrency', 'checkpoint', 'onParentError', 'responseMetadata', 'rawResponseDestination', 'httpProxy', 'httpUserAgent', 'httpConnectTimeoutSecs', 'httpReadTimeoutSecs'],
    },
    'src.synapse': {
        'kind': 'source',
        'summary': 'Azure Synapse rides the SQL Server TDS wire - same connection form as src.sqlserver.',
        'params': ['connectionRef', 'host', 'port', 'user', 'password', 'database', 'trustCert', 'encrypt', 'schema', 'tableName', 'query'],
    },
    'src.telegram': {
        'kind': 'source',
        'summary': 'Telegram Bot API. Token in URL path (https://api.telegram.org/bot{token}/getUpdates). Offset pagination via `?offset=N`. responsePath /result. No auth header needed - token is in the URL.',
        'params': ['url', 'method', 'body', 'headers', 'connectionRef', 'transportRef', 'authType', 'authToken', 'authHeader', 'tokenUrl', 'clientId', 'clientSecret', 'clientAuth', 'scope', 'responsePath', 'jsonPath', 'paginationType', 'cursorNextPath', 'cursorParam', 'offsetParam', 'pageSize', 'totalCountPath', 'pageParam', 'startPage', 'maxPages', 'urlTemplate', 'parentKeyColumn', 'maxRequests', 'incrementalField', 'incrementalInitial', 'concurrency', 'checkpoint', 'onParentError', 'responseMetadata', 'rawResponseDestination', 'httpProxy', 'httpUserAgent', 'httpConnectTimeoutSecs', 'httpReadTimeoutSecs'],
    },
    'src.teradata': {
        'kind': 'source',
        'summary': 'Read from Teradata through its free ODBC driver (no DuckDB Teradata extension exists). Install the Teradata ODBC driver, then connect with friendly host / user / password / database fields, a DSN, or a full ODBC connection string. Whole-table read or custom SQL; types preserved.',
        'params': ['driver', 'host', 'user', 'password', 'database', 'dsn', 'connectionString', 'query', 'tableName'],
    },
    'src.toml': {
        'kind': 'source',
        'summary': 'Read a TOML file as a table. Top-level TOML doc becomes one row (TOML disallows a top-level array). Suits Cargo / pyproject / Hugo config audits.',
        'params': ['path', 'encoding', 'glob'],
    },
    'src.trello': {
        'kind': 'source',
        'summary': 'Trello REST. Anonymous-style auth: append `?key={apiKey}&token={token}` to the URL. No body, no pagination (the API returns full result sets by default). Set responsePath empty since responses are top-level arrays. Base URL https://api.trello.com/1.',
        'params': ['url', 'method', 'body', 'headers', 'connectionRef', 'transportRef', 'authType', 'authToken', 'authHeader', 'tokenUrl', 'clientId', 'clientSecret', 'clientAuth', 'scope', 'responsePath', 'jsonPath', 'paginationType', 'cursorNextPath', 'cursorParam', 'offsetParam', 'pageSize', 'totalCountPath', 'pageParam', 'startPage', 'maxPages', 'urlTemplate', 'parentKeyColumn', 'maxRequests', 'incrementalField', 'incrementalInitial', 'concurrency', 'checkpoint', 'onParentError', 'responseMetadata', 'rawResponseDestination', 'httpProxy', 'httpUserAgent', 'httpConnectTimeoutSecs', 'httpReadTimeoutSecs'],
    },
    'src.tsv': {
        'kind': 'source',
        'summary': 'Read tab-separated files',
        'params': ['path', 'encoding', 'glob', 'hasHeader', 'delimiter', 'quoteChar', 'skipLines', 'dateFormat', 'timestampFormat', 'filename', 'ignoreErrors', 'nullPadding', 'readOptions', 'partitionBy'],
    },
    'src.turso': {
        'kind': 'source',
        'summary': 'Read a Turso (libSQL) database over the HTTP pipeline API - no driver install. Paste the libsql:// URL the dashboard gives you (it is normalized to https) plus a database auth token. Whole-table read or custom SQL.',
        'params': ['url', 'authToken', 'tableName', 'query'],
    },
    'src.twilio': {
        'kind': 'source',
        'summary': 'Twilio REST. Basic auth (Account SID + Auth Token). Page-cursor pagination via `next_page_uri`. responsePath depends on resource (e.g. /messages, /calls). Base URL https://api.twilio.com/2010-04-01/Accounts/{AccountSid}.',
        'params': ['url', 'method', 'body', 'headers', 'connectionRef', 'transportRef', 'authType', 'authToken', 'authHeader', 'tokenUrl', 'clientId', 'clientSecret', 'clientAuth', 'scope', 'responsePath', 'jsonPath', 'paginationType', 'cursorNextPath', 'cursorParam', 'offsetParam', 'pageSize', 'totalCountPath', 'pageParam', 'startPage', 'maxPages', 'urlTemplate', 'parentKeyColumn', 'maxRequests', 'incrementalField', 'incrementalInitial', 'concurrency', 'checkpoint', 'onParentError', 'responseMetadata', 'rawResponseDestination', 'httpProxy', 'httpUserAgent', 'httpConnectTimeoutSecs', 'httpReadTimeoutSecs'],
    },
    'src.vortex': {
        'kind': 'source',
        'summary': 'Read Vortex columnar files (.vortex) via the bundled duckle-lance sidecar. Vortex is a next-gen columnar format with fast random access; the sidecar bridges it into the engine through Parquet.',
        'params': ['path'],
    },
    'src.weaviate': {
        'kind': 'source',
        'summary': "List Weaviate objects via GET /v1/objects?class=&after=. Cursor pagination on the last object's id; emits {id, ...properties[, vector]} rows. apiKey via Bearer.",
        'params': ['connectionRef', 'endpoint', 'class', 'apiKey', 'pageSize', 'maxPages', 'withVector'],
    },
    'src.webhook': {
        'kind': 'source',
        'summary': 'Bind 127.0.0.1:port and collect up to `maxRequests` inbound HTTP requests with a global `timeoutMs` deadline. JSON-object bodies become the row; JSON-array bodies unfold into rows; other bodies fall back to {method, path, body, headers}. Local-only by design - point a tunnel (ngrok / cloudflared)...',
        'params': ['port', 'maxRequests', 'timeoutMs', 'pathFilter'],
    },
    'src.websocket': {
        'kind': 'source',
        'summary': 'Connect to a ws:// or wss:// URL, optionally send a subscribe frame, and collect up to maxMessages frames (or until timeoutMs). JSON object -> one row, JSON array -> a row each, other text -> {message}. For live feeds (market data, sensor streams). Batch ETL semantics.',
        'params': ['url', 'subscribe', 'headers', 'maxMessages', 'timeoutMs'],
    },
    'src.xero': {
        'kind': 'source',
        'summary': 'Xero REST. Either paste a Bearer OAuth token, or pick OAuth 2.0 Client Credentials and give the token URL (https://identity.xero.com/connect/token) with HTTP Basic client auth so a fresh token is minted per run - that suits a Xero Custom Connection. Pass Xero-Tenant-Id as a custom header. respons...',
        'params': ['url', 'method', 'body', 'headers', 'connectionRef', 'transportRef', 'authType', 'authToken', 'authHeader', 'tokenUrl', 'clientId', 'clientSecret', 'clientAuth', 'scope', 'responsePath', 'jsonPath', 'paginationType', 'cursorNextPath', 'cursorParam', 'offsetParam', 'pageSize', 'totalCountPath', 'pageParam', 'startPage', 'maxPages', 'urlTemplate', 'parentKeyColumn', 'maxRequests', 'incrementalField', 'incrementalInitial', 'concurrency', 'checkpoint', 'onParentError', 'responseMetadata', 'rawResponseDestination', 'httpProxy', 'httpUserAgent', 'httpConnectTimeoutSecs', 'httpReadTimeoutSecs'],
    },
    'src.xml': {
        'kind': 'source',
        'summary': 'Read XML files via the pure-Rust `quick-xml` parser. rowPath is a slash-separated element walk (e.g. `library/books/book`); every matching element becomes one row. Attributes prefix with `@`, text content goes to `_text`, nested children nest; repeated same-name siblings collapse to arrays.',
        'params': ['path', 'encoding', 'glob', 'rowPath', 'namespace', 'xsdPath', 'xsdChangePolicy', 'password', 'privateKey', 'keyPassphrase', 'hostFingerprint', 'uriColumn', 'carryColumns', 'shaColumn', 'onError', 'accessKey', 'secretKey', 'sessionToken', 'region', 'endpoint', 'urlStyle', 'useSsl', 'headers', 'user', 'cacheOutput'],
    },
    'src.yaml': {
        'kind': 'source',
        'summary': 'Read a YAML file as a table. Top-level YAML arrays become one row per element; non-array docs become a single row. Suits config-data ETL (Helm values, GitHub Actions matrices) not bulk logs.',
        'params': ['path', 'encoding', 'glob'],
    },
    'src.zendesk': {
        'kind': 'source',
        'summary': 'Zendesk Support REST. Basic auth (email/token + API token). Cursor pagination via `meta.after_cursor` + `page[after]` param. responsePath /tickets (or whatever resource).',
        'params': ['url', 'method', 'body', 'headers', 'connectionRef', 'transportRef', 'authType', 'authToken', 'authHeader', 'tokenUrl', 'clientId', 'clientSecret', 'clientAuth', 'scope', 'responsePath', 'jsonPath', 'paginationType', 'cursorNextPath', 'cursorParam', 'offsetParam', 'pageSize', 'totalCountPath', 'pageParam', 'startPage', 'maxPages', 'urlTemplate', 'parentKeyColumn', 'maxRequests', 'incrementalField', 'incrementalInitial', 'concurrency', 'checkpoint', 'onParentError', 'responseMetadata', 'rawResponseDestination', 'httpProxy', 'httpUserAgent', 'httpConnectTimeoutSecs', 'httpReadTimeoutSecs'],
    },
    'xf.addcol': {
        'kind': 'transform',
        'summary': '',
        'params': ['name', 'type', 'expression'],
    },
    'xf.aggwin': {
        'kind': 'transform',
        'summary': 'Aggregate over a window, keep every row',
        'params': ['function', 'column', 'partitionBy', 'orderBy', 'outputName'],
    },
    'xf.ai.chunk': {
        'kind': 'transform',
        'summary': 'Split long text into chunks for RAG / embedding pipelines. No API call - pure local char-window splitting with overlap. Props: inputColumn (default `text`), outputColumn (default `chunk`), chunkSize (default 1000), chunkOverlap (default 100), mode (`explode` = one row per chunk with chunk_index/c...',
        'params': ['inputColumn', 'strategy', 'chunkSize', 'outputColumn'],
        'unverified': ['overlap'],
    },
    'xf.ai.classify': {
        'kind': 'transform',
        'summary': 'Per-row LLM-backed classification. Props: inputColumn (default `text`), outputColumn (default `category`), categories (required, comma-separated list), model (default `gpt-4o-mini`), apiKey, baseUrl. The model is prompted to pick exactly one category; anything outside the list normalizes to `UNKN...',
        'params': ['inputColumn', 'categories', 'model', 'apiKey', 'outputColumn', 'baseUrl', 'endpointPath', 'headers', 'concurrency', 'checkpoint', 'checkpointKey', 'checkpointFingerprint', 'maxRetries', 'maxRequests', 'maxInputTokens', 'maxOutputTokens', 'maxEstimatedCostUsd', 'inputUsdPerMillionTokens', 'outputUsdPerMillionTokens'],
        'unverified': ['provider'],
    },
    'xf.ai.dedupe': {
        'kind': 'transform',
        'summary': 'Drop near-duplicate rows by cosine similarity over a pre-computed embedding column (typically from xf.ai.embed upstream). Props: embeddingColumn (default `embedding`), threshold (default 0.95). No API call; pure local math. O(N^2) - chain after xf.rows.head if your dataset is huge.',
        'params': ['embeddingColumn', 'textColumn', 'threshold', 'metric', 'keep'],
    },
    'xf.ai.embed': {
        'kind': 'transform',
        'summary': 'Per-row embedding via any OpenAI-compatible /v1/embeddings endpoint. Props: inputColumn (default `text`), outputColumn (default `embedding`), model (default `text-embedding-3-small`), apiKey (required, sent as Bearer), baseUrl (default `https://api.openai.com` - point at Cohere, Voyage, llama.cpp...',
        'params': ['inputColumn', 'model', 'apiKey', 'outputColumn', 'dimension', 'batchSize', 'concurrency', 'checkpoint', 'checkpointKey', 'checkpointFingerprint', 'maxRetries', 'maxRequests', 'maxInputTokens', 'maxOutputTokens', 'maxEstimatedCostUsd', 'inputUsdPerMillionTokens', 'outputUsdPerMillionTokens', 'baseUrl', 'endpointPath', 'headers'],
        'unverified': ['provider'],
    },
    'xf.ai.llm': {
        'kind': 'transform',
        'summary': 'Per-row LLM completion via any OpenAI-compatible /v1/chat/completions endpoint. Props: promptTemplate with `{column}` substitution (or inputColumn for passthrough), outputColumn (default `completion`), model (default `gpt-4o-mini`), apiKey (required), baseUrl, systemPrompt, temperature. One HTTP ...',
        'params': ['model', 'apiKey', 'baseUrl', 'endpointPath', 'headers', 'promptTemplate', 'outputColumn', 'temperature', 'maxTokens', 'concurrency', 'checkpoint', 'checkpointKey', 'checkpointFingerprint', 'maxRetries', 'maxRequests', 'maxInputTokens', 'maxOutputTokens', 'maxEstimatedCostUsd', 'inputUsdPerMillionTokens', 'outputUsdPerMillionTokens', 'responseFormat', 'jsonSchema', 'schemaName', 'expandColumns', 'onInvalid'],
        'unverified': ['provider'],
    },
    'xf.ai.pii': {
        'kind': 'transform',
        'summary': 'Regex-based PII redaction (email, phone, SSN, credit card). No API call. Props: inputColumn (default `text`), outputColumn (defaults to input - overwrites in place), types (comma-list subset; empty = all). LLM-backed redaction is a follow-up.',
        'params': ['columns', 'action'],
        'unverified': ['entities'],
    },
    'xf.ai.text_search': {
        'kind': 'transform',
        'summary': 'BM25 keyword search over text columns via DuckDB fts',
        'params': ['idColumn', 'textColumns', 'query', 'topK', 'outputColumn'],
    },
    'xf.ai.vector_search': {
        'kind': 'transform',
        'summary': 'Rank rows by similarity to a query vector via DuckDB vss',
        'params': ['vectorColumn', 'targetVector', 'dimension', 'distanceMetric', 'topK', 'outputColumn'],
    },
    'xf.anti': {
        'kind': 'transform',
        'summary': '',
        'params': ['leftKey', 'rightKey', 'multipleKeys', 'joinType'],
    },
    'xf.approx.quantile': {
        'kind': 'transform',
        'summary': 'Approximate quantile (median, p95, p99) via t-digest - fixed memory regardless of cardinality',
        'params': ['column', 'quantile', 'groupBy', 'outputColumn'],
    },
    'xf.archive.extract': {
        'kind': 'transform',
        'summary': 'Turn one archive artifact into one artifact per member. Reads a uri column of archives - ZIP, TAR, TAR.GZ or GZIP - and lands each member at an s3:// prefix or a local directory, emitting archive_uri / member_name / member_index / uri / media_type / compressed_size / size_bytes / sha256 so each m...',
        'params': ['uriColumn', 'destination', 'include', 'exclude', 'naming', 'ifExists', 'onError', 'maxMembers', 'maxUncompressedGb', 'partSizeMb', 'accessKey', 'secretKey', 'sessionToken', 'region', 'endpoint', 'urlStyle', 'useSsl', 'headers', 'user', 'password', 'privateKey', 'keyPassphrase', 'hostFingerprint'],
    },
    'xf.arr.collect': {
        'kind': 'transform',
        'summary': '',
        'params': ['valueColumn', 'groupBy', 'outputColumn'],
    },
    'xf.arr.contains': {
        'kind': 'transform',
        'summary': '',
        'params': ['column', 'value', 'outputColumn'],
    },
    'xf.arr.distinct': {
        'kind': 'transform',
        'summary': '',
        'params': ['column', 'outputColumn'],
    },
    'xf.arr.element': {
        'kind': 'transform',
        'summary': '',
        'params': ['column', 'index', 'outputColumn'],
    },
    'xf.arr.explode': {
        'kind': 'transform',
        'summary': '',
        'params': ['column'],
    },
    'xf.arr.length': {
        'kind': 'transform',
        'summary': 'Scalar length of a list / array column',
        'params': ['column', 'outputColumn'],
    },
    'xf.artifact.copy': {
        'kind': 'transform',
        'summary': 'Land the BYTES of the artifacts named upstream somewhere durable, and emit a row per landed copy. Reads a uri column (whatever src.changed, src.artifact or a query produced) and copies from https://, s3://, sftp:// or a local path to an s3:// prefix or a local directory. Streamed and hashed in ON...',
        'params': ['uriColumn', 'destination', 'naming', 'ifExists', 'partSizeMb', 'headers', 'user', 'password', 'privateKey', 'keyPassphrase', 'hostFingerprint', 'accessKey', 'secretKey', 'sessionToken', 'region', 'endpoint', 'urlStyle', 'useSsl'],
    },
    'xf.assert': {
        'kind': 'transform',
        'summary': 'Hard-fail the pipeline if any row violates a SQL predicate (defensive ETL check)',
        'params': ['predicate', 'message'],
    },
    'xf.audit': {
        'kind': 'transform',
        'summary': 'Append _loaded_at / _loaded_date / _source / _batch_id columns to every row. Standard warehouse provenance pattern',
        'params': ['loadedAt', 'loadedDate', 'source', 'batchId'],
    },
    'xf.case': {
        'kind': 'transform',
        'summary': '',
        'params': ['column', 'pattern', 'replacement', 'outputColumn'],
    },
    'xf.cast': {
        'kind': 'transform',
        'summary': '',
        'params': ['casts', 'onError'],
    },
    'xf.cdc.diff': {
        'kind': 'transform',
        'summary': 'Tag inserted/updated/deleted rows vs a previous snapshot',
        'params': ['naturalKey', 'compareColumns', 'rejectUnchanged'],
    },
    'xf.cdc.scd1': {
        'kind': 'transform',
        'summary': 'Resolved current state: cur + prev rows whose key is not in cur',
        'params': ['naturalKey', 'compareColumns', 'rejectUnchanged'],
    },
    'xf.cdc.scd2': {
        'kind': 'transform',
        'summary': 'Maintain versioned history: close changed rows, insert new versions',
        'params': ['naturalKey', 'compareColumns', 'validFromColumn', 'validToColumn', 'isCurrentColumn'],
    },
    'xf.cdc.scd3': {
        'kind': 'transform',
        'summary': 'Keep the PREVIOUS value of each tracked attribute in a sibling previous_<col> column. Main input is the current rows; connect the prior snapshot to the previous (lookup) port. Per tracked column, outputs current + previous_<col> joined on the key (NULL for new keys). Optional effective-date stamp.',
        'params': ['naturalKey', 'compareColumns', 'effectiveDateColumn'],
    },
    'xf.cdc.upsert': {
        'kind': 'transform',
        'summary': 'Emit the upsert payload: new + changed rows from cur',
        'params': ['naturalKey', 'compareColumns', 'rejectUnchanged'],
    },
    'xf.coalesce': {
        'kind': 'transform',
        'summary': 'Fill nulls via an expression',
        'params': ['name', 'type', 'expression'],
    },
    'xf.compare': {
        'kind': 'transform',
        'summary': 'Boolean column from comparing two row columns (=, !=, <, <=, >, >=)',
        'params': ['leftColumn', 'op', 'rightColumn', 'outputColumn'],
    },
    'xf.concat': {
        'kind': 'transform',
        'summary': '',
        'params': ['column', 'pattern', 'replacement', 'outputColumn'],
    },
    'xf.count': {
        'kind': 'transform',
        'summary': '',
        'params': ['groupKeys', 'aggregations', 'havingClause'],
    },
    'xf.cube': {
        'kind': 'transform',
        'summary': '',
        'params': ['groupKeys', 'aggregations', 'havingClause'],
    },
    'xf.cumulative': {
        'kind': 'transform',
        'summary': 'Running sum / avg / count / min / max over an ordered window',
        'params': ['column', 'function', 'orderBy', 'partitionBy', 'outputColumn'],
    },
    'xf.dbt': {
        'kind': 'transform',
        'summary': "Run dbt against the pipeline's DuckDB database. Either write one inline model right here (reference the upstream node as {{ var('duckle_input') }}), or set `projectDir` to an existing dbt project (folder with dbt_project.yml). The engine generates the dbt-duckdb profiles.yml automatically, so mod...",
        'params': ['model', 'modelName', 'projectDir', 'command', 'outputModel', 'schema', 'database', 'dbtBin', 'timeoutMs'],
    },
    'xf.denorm': {
        'kind': 'transform',
        'summary': 'Collapse rows per group, joining columns into delimited cells',
        'params': ['groupBy', 'aggregateColumns', 'separator'],
    },
    'xf.denserank': {
        'kind': 'transform',
        'summary': '',
        'params': ['function', 'targetColumn', 'offset', 'ntileBuckets', 'partitionBy', 'orderBy', 'outputName'],
    },
    'xf.diffsummary': {
        'kind': 'transform',
        'summary': 'Reduce a change feed (a change_type column, e.g. from DuckLake Data Diff) to a single summary row: added / removed / updated / total_changes counts plus a ready-made summary text. Feed it into LLM Transform for an AI narrative, or into a validator to assert expected counts in CI.',
        'params': ['changeColumn'],
    },
    'xf.distinct': {
        'kind': 'transform',
        'summary': 'Drop duplicate rows',
        'params': ['columns', 'orderBy'],
    },
    'xf.dropcol': {
        'kind': 'transform',
        'summary': '',
        'params': ['columns'],
    },
    'xf.dt.add': {
        'kind': 'transform',
        'summary': '',
        'params': ['column', 'amount', 'unit', 'outputColumn'],
    },
    'xf.dt.bin': {
        'kind': 'transform',
        'summary': 'Round timestamps down to fixed-interval buckets (e.g. 5 minutes, 1 hour) for time-series grouping',
        'params': ['column', 'count', 'unit', 'outputColumn'],
    },
    'xf.dt.diff': {
        'kind': 'transform',
        'summary': '',
        'params': ['startColumn', 'endColumn', 'unit', 'outputColumn'],
    },
    'xf.dt.epoch': {
        'kind': 'transform',
        'summary': 'Convert a TIMESTAMP to Unix epoch seconds, or epoch seconds back to TIMESTAMP',
        'params': ['column', 'mode', 'outputColumn'],
    },
    'xf.dt.extract': {
        'kind': 'transform',
        'summary': '',
        'params': ['column', 'unit', 'outputColumn'],
    },
    'xf.dt.format': {
        'kind': 'transform',
        'summary': '',
        'params': ['column', 'format', 'outputColumn'],
    },
    'xf.dt.now': {
        'kind': 'transform',
        'summary': 'Add a column with the pipeline run time - the standard loaded_at / processed_at stamp',
        'params': ['outputColumn'],
    },
    'xf.dt.parse': {
        'kind': 'transform',
        'summary': '',
        'params': ['column', 'format', 'outputColumn'],
    },
    'xf.dt.trunc': {
        'kind': 'transform',
        'summary': '',
        'params': ['column', 'unit', 'outputColumn'],
    },
    'xf.dt.tz': {
        'kind': 'transform',
        'summary': '',
        'params': ['column', 'timezone', 'outputColumn'],
    },
    'xf.except': {
        'kind': 'transform',
        'summary': 'Rows in the first input only',
        'params': ['matchBy'],
    },
    'xf.fill_backward': {
        'kind': 'transform',
        'summary': 'Replace NULL values with the next non-null value within an ordered window (pandas-style bfill / fill up)',
        'params': ['column', 'orderBy', 'partitionBy'],
    },
    'xf.fill_constant': {
        'kind': 'transform',
        'summary': 'Replace NULL values with a literal value (numbers pass through unquoted; everything else is treated as a string)',
        'params': ['column', 'value'],
    },
    'xf.fill_forward': {
        'kind': 'transform',
        'summary': 'Replace NULL values with the most recent non-null value within an ordered window (time-series gap fill)',
        'params': ['column', 'orderBy', 'partitionBy'],
    },
    'xf.filter': {
        'kind': 'transform',
        'summary': 'WHERE-style row filter',
        'params': ['predicate'],
    },
    'xf.first': {
        'kind': 'transform',
        'summary': '',
        'params': ['function', 'targetColumn', 'offset', 'ntileBuckets', 'partitionBy', 'orderBy', 'outputName'],
    },
    'xf.format': {
        'kind': 'transform',
        'summary': '',
        'params': ['column', 'pattern', 'replacement', 'outputColumn'],
    },
    'xf.geo.area': {
        'kind': 'transform',
        'summary': 'Area of each polygon; auto-picks planar or spheroid metres from the CRS',
        'params': ['geomColumn', 'outputColumn'],
    },
    'xf.geo.buffer': {
        'kind': 'transform',
        'summary': 'A buffered geometry around each row (ST_Buffer)',
        'params': ['geomColumn', 'distance', 'outputColumn'],
    },
    'xf.geo.clip': {
        'kind': 'transform',
        'summary': 'Two-input overlay (#217): keeps every attribute of the input layer on the main input and replaces its geometry with the part inside the clip layer on the second input. The clip layer is dissolved with ST_Union_Agg first, so a feature spanning several clip polygons yields one row rather than one p...',
        'params': ['geomColumn', 'clipGeomColumn'],
    },
    'xf.geo.create': {
        'kind': 'transform',
        'summary': 'Build a geometry column from X/Y coordinates, WKT, or WKB (ST_Point / ST_GeomFromText / ST_GeomFromWKB)',
        'params': ['source', 'xColumn', 'yColumn', 'wktColumn', 'wkbColumn', 'crs', 'outputColumn', 'removeSource'],
    },
    'xf.geo.distance': {
        'kind': 'transform',
        'summary': 'Distance from each row to a target geometry; auto-picks planar or spheroid from the CRS',
        'params': ['geomColumn', 'targetWkt', 'outputColumn'],
    },
    'xf.geo.erase': {
        'kind': 'transform',
        'summary': 'Two-input overlay (#218): keeps every attribute of the input layer on the main input and subtracts the erase layer on the second input (ST_Difference). The erase layer is dissolved with ST_Union_Agg first, since differencing each feature in turn would only remove the last. Features left with no g...',
        'params': ['geomColumn', 'eraseGeomColumn'],
    },
    'xf.geo.flip': {
        'kind': 'transform',
        'summary': 'Swap X/Y of every vertex to fix lat,lon vs lon,lat order (ST_FlipCoordinates)',
        'params': ['geomColumn'],
    },
    'xf.geo.intersects': {
        'kind': 'transform',
        'summary': 'Boolean: does each row overlap a target geometry? (ST_Intersects)',
        'params': ['geomColumn', 'targetWkt', 'outputColumn'],
    },
    'xf.geo.length': {
        'kind': 'transform',
        'summary': 'Length of each line; auto-picks planar or spheroid metres from the CRS',
        'params': ['geomColumn', 'outputColumn'],
    },
    'xf.geo.perimeter': {
        'kind': 'transform',
        'summary': 'Perimeter of each polygon; auto-picks planar or spheroid metres from the CRS',
        'params': ['geomColumn', 'outputColumn'],
    },
    'xf.geo.reproject': {
        'kind': 'transform',
        'summary': 'Reproject a geometry column from one CRS to another (ST_Transform)',
        'params': ['geomColumn', 'sourceCrs', 'targetCrs', 'alwaysXy'],
    },
    'xf.geo.setcrs': {
        'kind': 'transform',
        'summary': 'Assign a CRS to geometry with missing/unknown CRS, without moving the coordinates (ST_SetCRS)',
        'params': ['geomColumn', 'crs'],
    },
    'xf.groupby': {
        'kind': 'transform',
        'summary': '',
        'params': ['groupKeys', 'aggregations', 'havingClause'],
    },
    'xf.hash': {
        'kind': 'transform',
        'summary': 'Hash a column (md5 / sha1 / sha256) for anonymization or deterministic IDs',
        'params': ['column', 'algorithm', 'outputColumn'],
    },
    'xf.incremental': {
        'kind': 'transform',
        'summary': 'Pass only rows whose watermark column (e.g. updated_at, id) is past the last successful run. The new high-water mark is saved to workspace state and advances only when the whole run succeeds - so reruns never skip rows that were not delivered.',
        'params': ['column', 'initialValue'],
    },
    'xf.intersect': {
        'kind': 'transform',
        'summary': 'Rows present in all inputs',
        'params': ['matchBy'],
    },
    'xf.ip.parse': {
        'kind': 'transform',
        'summary': 'Extract host / family / netmask / broadcast from IP or CIDR text via the inet extension',
        'params': ['column', 'kind', 'outputColumn'],
    },
    'xf.join': {
        'kind': 'transform',
        'summary': 'Inner / left / right / full outer join, chosen by the Type dropdown',
        'params': ['leftKey', 'rightKey', 'multipleKeys', 'joinType'],
    },
    'xf.join.cross': {
        'kind': 'transform',
        'summary': '',
        'params': ['leftKey', 'rightKey', 'multipleKeys', 'joinType'],
    },
    'xf.join.spatial': {
        'kind': 'transform',
        'summary': 'Two-input join whose predicate is ST_Intersects / Contains / Within / Touches / Crosses / Overlaps / Equals',
        'params': ['leftGeomColumn', 'rightGeomColumn', 'relation', 'joinType'],
    },
    'xf.jq': {
        'kind': 'transform',
        'summary': 'Transform a JSON column with a jq program (in-process jaq, no external jq)',
        'params': ['column', 'filter', 'outputColumn', 'onError'],
    },
    'xf.json.array_agg': {
        'kind': 'transform',
        'summary': 'Collapse rows into a JSON array per group (json_group_array)',
        'params': ['column', 'groupBy', 'outputColumn'],
    },
    'xf.json.flatten': {
        'kind': 'transform',
        'summary': '',
        'params': ['column'],
    },
    'xf.json.merge': {
        'kind': 'transform',
        'summary': '',
        'params': ['column', 'secondColumn', 'outputColumn'],
    },
    'xf.json.parse': {
        'kind': 'transform',
        'summary': '',
        'params': ['column', 'outputColumn'],
    },
    'xf.json.path': {
        'kind': 'transform',
        'summary': '',
        'params': ['column', 'path', 'outputColumn'],
    },
    'xf.json.stringify': {
        'kind': 'transform',
        'summary': '',
        'params': ['column', 'outputColumn'],
    },
    'xf.lag': {
        'kind': 'transform',
        'summary': '',
        'params': ['function', 'targetColumn', 'offset', 'ntileBuckets', 'partitionBy', 'orderBy', 'outputName'],
    },
    'xf.last': {
        'kind': 'transform',
        'summary': '',
        'params': ['function', 'targetColumn', 'offset', 'ntileBuckets', 'partitionBy', 'orderBy', 'outputName'],
    },
    'xf.lead': {
        'kind': 'transform',
        'summary': '',
        'params': ['function', 'targetColumn', 'offset', 'ntileBuckets', 'partitionBy', 'orderBy', 'outputName'],
    },
    'xf.length': {
        'kind': 'transform',
        'summary': '',
        'params': ['column', 'pattern', 'replacement', 'outputColumn'],
    },
    'xf.log': {
        'kind': 'transform',
        'summary': 'Pass rows through and print them to Output',
        'params': ['label', 'limit', 'columns'],
    },
    'xf.lookup': {
        'kind': 'transform',
        'summary': '',
        'params': ['leftKey', 'rightKey', 'multipleKeys', 'joinType'],
    },
    'xf.map': {
        'kind': 'transform',
        'summary': 'Visual row mapper with main + lookup inputs',
        'params': ['mode', 'expressions'],
    },
    'xf.norm': {
        'kind': 'transform',
        'summary': 'Explode a delimited or array column into rows',
        'params': ['column', 'separator'],
    },
    'xf.ntile': {
        'kind': 'transform',
        'summary': '',
        'params': ['function', 'targetColumn', 'offset', 'ntileBuckets', 'partitionBy', 'orderBy', 'outputName'],
    },
    'xf.num.abs': {
        'kind': 'transform',
        'summary': '',
        'params': ['column', 'argument', 'outputColumn'],
    },
    'xf.num.bucketize': {
        'kind': 'transform',
        'summary': 'Bin a numeric column into N equal-width buckets between low and high (width_bucket)',
        'params': ['column', 'bounds', 'labels', 'low', 'high', 'buckets', 'outputColumn'],
    },
    'xf.num.clamp': {
        'kind': 'transform',
        'summary': 'Clip values to a [low, high] range - cap outliers before stats',
        'params': ['column', 'low', 'high'],
    },
    'xf.num.log': {
        'kind': 'transform',
        'summary': '',
        'params': ['column', 'argument', 'outputColumn'],
    },
    'xf.num.mod': {
        'kind': 'transform',
        'summary': '',
        'params': ['column', 'argument', 'outputColumn'],
    },
    'xf.num.power': {
        'kind': 'transform',
        'summary': '',
        'params': ['column', 'argument', 'outputColumn'],
    },
    'xf.num.round': {
        'kind': 'transform',
        'summary': '',
        'params': ['column', 'argument', 'outputColumn'],
    },
    'xf.num.sign': {
        'kind': 'transform',
        'summary': 'Sign of a number: -1, 0, or +1',
        'params': ['column', 'outputColumn'],
    },
    'xf.num.sqrt': {
        'kind': 'transform',
        'summary': '',
        'params': ['column', 'argument', 'outputColumn'],
    },
    'xf.num.zscore': {
        'kind': 'transform',
        'summary': 'Per-row standardized value: (value - mean) / stddev across the whole input',
        'params': ['column', 'outputColumn'],
    },
    'xf.pivot': {
        'kind': 'transform',
        'summary': 'Rows to columns',
        'params': ['pivotColumn', 'valueColumn', 'groupBy', 'aggregation'],
    },
    'xf.project': {
        'kind': 'transform',
        'summary': '',
        'params': ['columns'],
    },
    'xf.rank': {
        'kind': 'transform',
        'summary': '',
        'params': ['function', 'targetColumn', 'offset', 'ntileBuckets', 'partitionBy', 'orderBy', 'outputName'],
    },
    'xf.rank.filter': {
        'kind': 'transform',
        'summary': 'Keep the top N rows per group, ordered by a column (row_number window + filter)',
        'params': ['partitionBy', 'orderBy', 'desc', 'n'],
    },
    'xf.regex': {
        'kind': 'transform',
        'summary': '',
        'params': ['column', 'pattern', 'replacement', 'outputColumn'],
    },
    'xf.regex.extract': {
        'kind': 'transform',
        'summary': 'Extract a capture group from a column via regexp_extract',
        'params': ['column', 'pattern', 'groupIndex', 'groupNames', 'outputColumn'],
    },
    'xf.regex.match': {
        'kind': 'transform',
        'summary': 'Boolean: does the regex match the column? (regexp_matches)',
        'params': ['column', 'pattern', 'outputColumn'],
    },
    'xf.rename': {
        'kind': 'transform',
        'summary': '',
        'params': ['mapping', 'mappingFile'],
    },
    'xf.reorder': {
        'kind': 'transform',
        'summary': '',
        'params': ['columns'],
    },
    'xf.rollup': {
        'kind': 'transform',
        'summary': '',
        'params': ['groupKeys', 'aggregations', 'havingClause'],
    },
    'xf.row_hash': {
        'kind': 'transform',
        'summary': 'Hash N columns into one fingerprint column. md5 / sha1 / sha256. Stable across runs - feed downstream diff / dedup / change detection',
        'params': ['columns', 'algorithm', 'outputColumn'],
    },
    'xf.rownum': {
        'kind': 'transform',
        'summary': 'ROW_NUMBER() over a window',
        'params': ['function', 'targetColumn', 'offset', 'ntileBuckets', 'partitionBy', 'orderBy', 'outputName'],
    },
    'xf.sample': {
        'kind': 'transform',
        'summary': 'Random row sample',
        'params': ['count', 'orderBy'],
    },
    'xf.semi': {
        'kind': 'transform',
        'summary': '',
        'params': ['leftKey', 'rightKey', 'multipleKeys', 'joinType'],
    },
    'xf.sessionize': {
        'kind': 'transform',
        'summary': 'Assign a session id to event rows by inactivity gap (clickstream / analytics prep): a new session starts when the time gap from the previous event in the partition exceeds the threshold. Emits session_id (per-partition running integer) and optionally session_seq (event index within the session).',
        'params': ['partitionBy', 'orderBy', 'gap', 'gapUnit', 'sessionColumn', 'emitSeq', 'seqColumn'],
    },
    'xf.skip': {
        'kind': 'transform',
        'summary': 'Drop the first N rows',
        'params': ['count', 'orderBy'],
    },
    'xf.sort': {
        'kind': 'transform',
        'summary': 'Order rows',
        'params': ['orderBy'],
    },
    'xf.split': {
        'kind': 'transform',
        'summary': 'Split a column into a LIST in one column - use Text to Columns for separate columns',
        'params': ['column', 'pattern', 'replacement', 'outputColumn'],
    },
    'xf.substring': {
        'kind': 'transform',
        'summary': '',
        'params': ['column', 'pattern', 'replacement', 'outputColumn'],
    },
    'xf.surrogatekey': {
        'kind': 'transform',
        'summary': 'Add a warehouse dimension key derived from the business/natural key columns: hash mode (md5 of the key, stable across runs so the same business key always maps to the same surrogate) or sequence mode (1..N integer ordered by the key). Unlike UUID (random per row), this is deterministic.',
        'params': ['keyColumns', 'mode', 'separator', 'outputColumn'],
    },
    'xf.text.base64': {
        'kind': 'transform',
        'summary': 'Encode a column to base64 text, or decode base64 back to bytes',
        'params': ['column', 'mode', 'outputColumn'],
    },
    'xf.text.match': {
        'kind': 'transform',
        'summary': 'Boolean: does the string contain / start with / end with a substring (DuckDB contains / starts_with / ends_with)',
        'params': ['column', 'needle', 'mode', 'outputColumn'],
    },
    'xf.text.padding': {
        'kind': 'transform',
        'summary': 'Left or right pad to a fixed length (zero-pad IDs, right-pad for fixed-width output)',
        'params': ['column', 'length', 'fill', 'side', 'outputColumn'],
    },
    'xf.text.repeat': {
        'kind': 'transform',
        'summary': 'Repeat a string column N times',
        'params': ['column', 'count', 'outputColumn'],
    },
    'xf.text.replace': {
        'kind': 'transform',
        'summary': 'Literal substring replace (no regex metacharacters)',
        'params': ['column', 'search', 'replacement', 'outputColumn'],
    },
    'xf.text.reverse': {
        'kind': 'transform',
        'summary': 'Reverse the characters of a string column',
        'params': ['column', 'outputColumn'],
    },
    'xf.text.similarity': {
        'kind': 'transform',
        'summary': 'Pairwise string similarity between two columns - levenshtein / damerau / jaccard / jaro-winkler',
        'params': ['leftColumn', 'rightColumn', 'algorithm', 'outputColumn'],
    },
    'xf.text.slug': {
        'kind': 'transform',
        'summary': 'Generate a URL-safe slug: lowercase + hyphens, no punctuation',
        'params': ['column', 'outputColumn'],
    },
    'xf.text.strip_html': {
        'kind': 'transform',
        'summary': 'Remove HTML tags from a column (regex-based, keeps the text content)',
        'params': ['column', 'outputColumn'],
    },
    'xf.text.tocolumns': {
        'kind': 'transform',
        'summary': 'Split a delimited column into separate named columns (split_part), e.g. "31.21 30.24" into latitude and longitude',
        'params': ['column', 'delimiter', 'outputColumns', 'dropSource'],
    },
    'xf.topn': {
        'kind': 'transform',
        'summary': 'Keep the first N rows',
        'params': ['count', 'orderBy'],
    },
    'xf.transpose': {
        'kind': 'transform',
        'summary': 'Swap rows and columns',
        'params': [],
    },
    'xf.trim': {
        'kind': 'transform',
        'summary': '',
        'params': ['column', 'pattern', 'replacement', 'outputColumn'],
    },
    'xf.tumble': {
        'kind': 'transform',
        'summary': 'Event-time tumbling windows that survive across runs. Rows are held until their window CLOSES, decided by a watermark (the greatest event time seen so far) rather than the wall clock - so replaying old data produces the windows that data belongs to instead of closing them all at once. Adds window...',
        'params': ['timeColumn', 'size', 'allowedLateness'],
    },
    'xf.union': {
        'kind': 'transform',
        'summary': 'Combine inputs, drop duplicates',
        'params': ['matchBy'],
    },
    'xf.unionall': {
        'kind': 'transform',
        'summary': 'Combine inputs, keep all rows',
        'params': ['matchBy'],
    },
    'xf.unpivot': {
        'kind': 'transform',
        'summary': 'Columns to name/value rows (wide to long)',
        'params': ['columns', 'nameColumn', 'valueColumn'],
    },
    'xf.url.parse': {
        'kind': 'transform',
        'summary': 'Extract scheme / host / port / path / query / fragment from a URL column',
        'params': ['column', 'kind', 'outputColumn'],
    },
    'xf.uuid': {
        'kind': 'transform',
        'summary': 'Add a fresh UUID v4 column per row - the standard surrogate row id',
        'params': ['outputColumn'],
    },
    'xf.zip': {
        'kind': 'transform',
        'summary': 'Zip a headings list and a list of row-arrays (e.g. {headings:[...], rows:[[...]]}) into one row per record with a real column per heading',
        'params': ['headingsColumn', 'valuesColumn'],
    },
}
