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
    | 'rest';

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
    accountName?: string;
    accountKey?: string;
    brokers?: string;
    url?: string;
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
