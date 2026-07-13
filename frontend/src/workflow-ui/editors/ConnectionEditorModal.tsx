import { useEffect, useMemo, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { Plug, Save, X } from 'lucide-react';
import type { ConnectionKind, ConnectionPayload, RepoItem } from '../../repo-types';

type Props = {
    item: RepoItem | null;
    onSave: (name: string, payload: ConnectionPayload) => void;
    onCancel: () => void;
};

type ConnectionType = {
    kind: ConnectionKind;
    label: string;
    fields: Array<keyof ConnectionPayload>;
    defaultPort?: number;
};

const CONNECTION_TYPES: ConnectionType[] = [
    {
        kind: 'postgres',
        label: 'PostgreSQL',
        fields: [
            'host', 'port', 'database', 'username', 'password',
            // Advanced / TLS (issue #161).
            'sslmode', 'sslrootcert', 'sslcert', 'sslkey', 'connectTimeout', 'options', 'connParams',
        ],
        defaultPort: 5432,
    },
    {
        kind: 'mysql',
        label: 'MySQL',
        fields: ['host', 'port', 'database', 'username', 'password'],
        defaultPort: 3306,
    },
    {
        kind: 'mariadb',
        label: 'MariaDB',
        fields: ['host', 'port', 'database', 'username', 'password'],
        defaultPort: 3306,
    },
    {
        kind: 'sqlserver',
        label: 'SQL Server',
        fields: ['host', 'port', 'database', 'username', 'password'],
        defaultPort: 1433,
    },
    {
        kind: 'oracle',
        label: 'Oracle',
        fields: ['host', 'port', 'database', 'username', 'password'],
        defaultPort: 1521,
    },
    { kind: 'sqlite', label: 'SQLite', fields: ['database'] },
    { kind: 'duckdb', label: 'DuckDB', fields: ['database'] },
    {
        kind: 'snowflake',
        label: 'Snowflake',
        fields: ['host', 'database', 'username', 'password'],
    },
    {
        kind: 'bigquery',
        label: 'BigQuery',
        fields: ['accountName', 'database'],
    },
    {
        kind: 'redshift',
        label: 'Redshift',
        fields: [
            'host', 'port', 'database', 'username', 'password',
            'sslmode', 'sslrootcert', 'sslcert', 'sslkey', 'connectTimeout', 'options', 'connParams',
        ],
        defaultPort: 5439,
    },
    {
        kind: 'clickhouse',
        label: 'ClickHouse',
        fields: ['host', 'port', 'database', 'username', 'password'],
        defaultPort: 8123,
    },
    {
        kind: 'mongodb',
        label: 'MongoDB',
        fields: ['host', 'port', 'database', 'username', 'password'],
        defaultPort: 27017,
    },
    {
        kind: 'redis',
        label: 'Redis',
        fields: ['host', 'port', 'password'],
        defaultPort: 6379,
    },
    {
        kind: 'elastic',
        label: 'Elasticsearch',
        fields: ['host', 'port', 'username', 'password'],
        defaultPort: 9200,
    },
    {
        kind: 's3',
        label: 'Amazon S3 / MinIO',
        fields: ['bucket', 'region', 'accessKey', 'secretKey', 'endpoint', 'urlStyle'],
    },
    {
        kind: 'gcs',
        label: 'Google Cloud Storage',
        fields: ['bucket', 'accountName'],
    },
    {
        kind: 'azure-blob',
        label: 'Azure Blob Storage',
        fields: ['accountName', 'accountKey', 'bucket'],
    },
    { kind: 'kafka', label: 'Kafka', fields: ['brokers', 'username', 'password'] },
    { kind: 'rest', label: 'REST API', fields: ['url'] },
    {
        // #166 stage 2: field names match what the engine's Salesforce
        // connectors read, so run-time resolution injects them verbatim.
        kind: 'salesforce',
        label: 'Salesforce',
        fields: ['authMode', 'loginUrl', 'instanceUrl', 'clientId', 'clientSecret', 'accessToken'],
    },
];

const FIELD_LABELS: Partial<Record<keyof ConnectionPayload, string>> = {
    host: 'Host',
    port: 'Port',
    database: 'Database',
    username: 'Username',
    password: 'Password',
    bucket: 'Bucket',
    region: 'Region',
    accessKey: 'Access key',
    secretKey: 'Secret key',
    accountName: 'Account / Project',
    accountKey: 'Account key',
    brokers: 'Bootstrap servers',
    url: 'Base URL',
    endpoint: 'Endpoint (MinIO / R2 / B2, blank for AWS)',
    urlStyle: 'URL style',
    sslmode: 'SSL mode',
    sslrootcert: 'SSL root cert',
    sslcert: 'SSL client cert',
    sslkey: 'SSL client key',
    connectTimeout: 'Connect timeout (s)',
    options: 'Session options',
    connParams: 'Extra parameters',
    authMode: 'Auth mode',
    loginUrl: 'Login URL (Client Credentials)',
    instanceUrl: 'Instance URL (Bearer mode)',
    clientId: 'Client ID (Client Credentials)',
    clientSecret: 'Client secret (Client Credentials)',
    accessToken: 'Access token (Bearer mode)',
};

const SECRET_FIELDS = new Set<keyof ConnectionPayload>([
    'password',
    'secretKey',
    'accountKey',
    'clientSecret',
    'accessToken',
]);

export default function ConnectionEditorModal({ item, onSave, onCancel }: Props) {
    const initial = (item?.payload as ConnectionPayload | undefined) ?? null;
    const [name, setName] = useState(item?.name ?? '');
    const [kind, setKind] = useState<ConnectionKind>(initial?.kind ?? 'postgres');
    const [values, setValues] = useState<ConnectionPayload>(initial ?? { kind: 'postgres' });
    const nameRef = useRef<HTMLInputElement>(null);

    const meta = useMemo(() => CONNECTION_TYPES.find(c => c.kind === kind), [kind]);

    useEffect(() => {
        setTimeout(() => nameRef.current?.focus(), 30);
        const onKey = (e: KeyboardEvent) => {
            if (e.key === 'Escape') onCancel();
        };
        document.addEventListener('keydown', onKey);
        return () => document.removeEventListener('keydown', onKey);
    }, [onCancel]);

    const setField = (key: keyof ConnectionPayload, value: string | number) => {
        setValues(v => ({ ...v, [key]: value }));
    };

    const handleKindChange = (newKind: ConnectionKind) => {
        setKind(newKind);
        const m = CONNECTION_TYPES.find(c => c.kind === newKind);
        setValues(v => ({
            ...v,
            kind: newKind,
            port: m?.defaultPort ?? v.port,
        }));
    };

    const canSave = name.trim().length > 0;

    const handleSave = () => {
        if (!canSave) return;
        onSave(name.trim(), { ...values, kind });
    };

    return createPortal(
        <div
            className="modal-backdrop"
            onClick={e => {
                if (e.target === e.currentTarget) onCancel();
            }}
        >
            <div className="modal modal-editor">
                <div className="modal-header">
                    <div className="modal-title-row">
                        <Plug size={16} className="modal-title-icon" />
                        <div>
                            <div className="modal-title">
                                {item ? 'Edit connection' : 'New connection'}
                            </div>
                            <div className="modal-subtitle">
                                {meta?.label ?? kind} · saved in <code>Connections</code>
                            </div>
                        </div>
                    </div>
                    <button
                        type="button"
                        className="modal-close"
                        onClick={onCancel}
                        aria-label="Close"
                    >
                        <X size={16} />
                    </button>
                </div>

                <div className="modal-body">
                    <div className="modal-field">
                        <label className="modal-field-label">Connection name</label>
                        <input
                            ref={nameRef}
                            type="text"
                            className="modal-input"
                            value={name}
                            placeholder="e.g. analytics_warehouse_prod"
                            onChange={e => setName(e.target.value)}
                            spellCheck={false}
                        />
                    </div>

                    <div className="modal-field">
                        <label className="modal-field-label">Type</label>
                        <select
                            className="modal-input modal-select"
                            value={kind}
                            onChange={e => handleKindChange(e.target.value as ConnectionKind)}
                        >
                            {CONNECTION_TYPES.map(c => (
                                <option key={c.kind} value={c.kind}>
                                    {c.label}
                                </option>
                            ))}
                        </select>
                    </div>

                    <div className="connection-field-grid">
                        {meta?.fields.map(field => {
                            const isSecret = SECRET_FIELDS.has(field);
                            const isNumber = field === 'port' || field === 'connectTimeout';
                            if (field === 'sslmode') {
                                // Values MUST match libpq sslmode names so the
                                // engine forwards them verbatim into the DSN (#161).
                                return (
                                    <div className="modal-field" key={field}>
                                        <label className="modal-field-label">
                                            {FIELD_LABELS[field] ?? field}
                                        </label>
                                        <select
                                            className="modal-input"
                                            value={(values.sslmode as string | undefined) ?? ''}
                                            onChange={e => setField('sslmode', e.target.value)}
                                        >
                                            <option value="">Default</option>
                                            <option value="disable">disable</option>
                                            <option value="allow">allow</option>
                                            <option value="prefer">prefer</option>
                                            <option value="require">require</option>
                                            <option value="verify-ca">verify-ca</option>
                                            <option value="verify-full">verify-full</option>
                                        </select>
                                    </div>
                                );
                            }
                            if (field === 'authMode') {
                                // The values MUST match what the run-time
                                // resolver maps onto the node's authMode /
                                // authType props (#166 stage 2).
                                return (
                                    <div className="modal-field" key={field}>
                                        <label className="modal-field-label">
                                            {FIELD_LABELS[field] ?? field}
                                        </label>
                                        <select
                                            className="modal-input"
                                            value={(values.authMode as string | undefined) ?? 'bearer'}
                                            onChange={e => setField('authMode', e.target.value)}
                                        >
                                            <option value="bearer">Bearer token (paste / refresh manually)</option>
                                            <option value="clientCredentials">OAuth 2.0 Client Credentials (mint per run)</option>
                                        </select>
                                    </div>
                                );
                            }
                            if (field === 'urlStyle') {
                                // The values MUST match the S3 node's URL-style
                                // option values ('' / 'path' / 'vhost') so picking
                                // this saved connection on a node lands on a real
                                // option instead of falling back to Default (#116).
                                return (
                                    <div className="modal-field" key={field}>
                                        <label className="modal-field-label">
                                            {FIELD_LABELS[field] ?? field}
                                        </label>
                                        <select
                                            className="modal-input"
                                            value={(values.urlStyle as string | undefined) ?? ''}
                                            onChange={e => setField('urlStyle', e.target.value)}
                                        >
                                            <option value="">Default</option>
                                            <option value="path">Path (MinIO / B2)</option>
                                            <option value="vhost">Virtual host (R2 / AWS)</option>
                                        </select>
                                    </div>
                                );
                            }
                            return (
                                <div className="modal-field" key={field}>
                                    <label className="modal-field-label">
                                        {FIELD_LABELS[field] ?? field}
                                    </label>
                                    <input
                                        type={isSecret ? 'password' : isNumber ? 'number' : 'text'}
                                        className="modal-input"
                                        value={(values[field] as string | number | undefined) ?? ''}
                                        placeholder={isSecret ? '••••••••' : ''}
                                        onChange={e =>
                                            setField(
                                                field,
                                                isNumber ? Number(e.target.value) : e.target.value,
                                            )
                                        }
                                        spellCheck={false}
                                    />
                                </div>
                            );
                        })}
                    </div>

                    <div className="modal-field">
                        <label className="modal-field-label">Notes (optional)</label>
                        <textarea
                            className="modal-input"
                            rows={2}
                            value={values.notes ?? ''}
                            onChange={e => setValues(v => ({ ...v, notes: e.target.value }))}
                            spellCheck={false}
                        />
                    </div>
                </div>

                <div className="modal-footer">
                    <button type="button" className="btn btn-secondary" onClick={onCancel}>
                        Cancel
                    </button>
                    <button
                        type="button"
                        className="btn btn-primary"
                        onClick={handleSave}
                        disabled={!canSave}
                    >
                        <Save size={13} />
                        Save
                    </button>
                </div>
            </div>
        </div>,
        document.body,
    );
}
