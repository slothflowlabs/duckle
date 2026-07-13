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
    | 'salesforce';

export type ConnectionPayload = {
    kind: ConnectionKind;
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
