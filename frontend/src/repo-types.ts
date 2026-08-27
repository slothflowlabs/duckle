export type RepoItemType =
    | 'project'
    | 'folder'
    | 'pipeline'
    | 'connection'
    | 'context'
    | 'routine'
    | 'doc'
    | 'dive'
    | 'dashboard';

// ---- Per-type payloads ----

export type ConnectionKind =
    | 'postgres'
    | 'mysql'
    | 'mariadb'
    | 'sqlserver'
    | 'oracle'
    | 'sqlite'
    | 'duckdb'
    | 'snowflake'
    | 'bigquery'
    | 'redshift'
    | 'clickhouse'
    | 'mongodb'
    | 'redis'
    | 'elastic'
    | 's3'
    | 'gcs'
    | 'azure-blob'
    | 'kafka'
    | 'rest'
    | 'salesforce'
    // #256: transport, not credentials - proxy, timeouts, User-Agent - shared by
    // every HTTP-backed component instead of being set on each node.
    | 'http';

export type ConnectionPayload = {
    kind: ConnectionKind;
    // #256: HTTP transport (kind: 'http'). Flattened onto a node's
    // httpProxy / httpReadTimeoutSecs / httpConnectTimeoutSecs / httpUserAgent
    // at run time, with anything the node set itself left alone.
    proxy?: string;
    readTimeoutSecs?: number;
    connectTimeoutSecs?: number;
    userAgent?: string;
    host?: string;
    port?: number;
    database?: string;
    username?: string;
    password?: string;
    bucket?: string;
    region?: string;
    accessKey?: string;
    secretKey?: string;
    // S3-compatible endpoint (MinIO / R2 / B2) + addressing style (#116).
    endpoint?: string;
    urlStyle?: string;
    // A MinIO on plain HTTP, and temporary STS credentials. Both are things the
    // engine reads and the form had no way to say, so a connection could not
    // describe either one.
    useSsl?: string;
    sessionToken?: string;
    accountName?: string;
    accountKey?: string;
    brokers?: string;
    url?: string;
    // PostgreSQL advanced / libpq TLS options (issue #161). Carried on the
    // connection so TLS-enforced servers (sslmode=require / verify-full) and
    // client-cert auth work from a saved connection, not just inline node props.
    sslmode?: string;
    sslrootcert?: string;
    sslcert?: string;
    sslkey?: string;
    connectTimeout?: number;
    options?: string;
    connParams?: string;
    // Salesforce OAuth (#166 stage 2). The node stores only a connectionRef;
    // the host resolves + decrypts these at run time, so the secret never
    // lands in the pipeline JSON. Field names match what the engine reads.
    // Saved REST connection (community request: vendor auth in one place, so
    // rotating a key is one edit instead of one per node). The node keeps only
    // a connectionRef; headers merge per key at run time and the node's own
    // url/body always win. See merge_rest_connection in duckle-secrets.
    headers?: { key: string; value: string }[];
    authType?: string;
    authToken?: string;
    authHeader?: string;
    tokenUrl?: string;
    authMode?: string;
    loginUrl?: string;
    instanceUrl?: string;
    clientId?: string;
    clientSecret?: string;
    accessToken?: string;
    extra?: Record<string, string>;
    notes?: string;
};

export type ContextVariable = {
    key: string;
    value: string;
    secret?: boolean;
};

export type ContextPayload = {
    variables: ContextVariable[];
    description?: string;
    /**
     * Layer this context sits on when several are active (#204). Higher wins.
     * Absent or 0 is the base layer.
     *
     * Contexts used to merge flat in repo order, so a shared base plus a
     * per-environment override could only be expressed by having one silently
     * outrank the other, and every intended override looked like a collision.
     * Give the base 0 and the environment a higher number and the override is
     * declared rather than incidental: it applies quietly, and only two
     * contexts on the SAME layer are still reported as ambiguous.
     */
    priority?: number;
};

export type DocumentPayload = {
    content: string;
};

export type RoutineLanguage = 'python' | 'rust' | 'javascript' | 'bash' | 'sql';

export type RoutinePayload = {
    language: RoutineLanguage;
    code: string;
    description?: string;
};

export type RepoPayload =
    | ConnectionPayload
    | ContextPayload
    | DocumentPayload
    | RoutinePayload
    | undefined;

export type RepoItem = {
    id: string;
    name: string;
    type: RepoItemType;
    parentId?: string;
    icon?: string;
    payload?: RepoPayload;
};
