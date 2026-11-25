import { useCallback, useEffect, useState } from 'react';
import { createPortal } from 'react-dom';
import { Boxes, CheckCircle2, Cpu, Download, Loader2, Workflow } from 'lucide-react';
import {
    dbtInstall,
    dbtStatus,
    engineInstall,
    engineStatus,
    type EngineStatus,
    type InstallProgress,
} from '../tauri-bridge';

type Props = {
    onReady: () => void;
};

export default function EngineSetupModal({ onReady }: Props) {
    const [engines, setEngines] = useState<EngineStatus[]>([]);
    const [loading, setLoading] = useState(true);
    const [progress, setProgress] = useState<Record<string, InstallProgress>>({});
    const [installing, setInstalling] = useState<string | null>(null);
    const [error, setError] = useState<string | null>(null);
    // dbt Fusion provisions through its own command (not the GitHub-zip engine
    // path), so it gets a dedicated row + state rather than an EngineStatus.
    const [dbtInstalled, setDbtInstalled] = useState(false);
    const [dbtBusy, setDbtBusy] = useState(false);
    const [dbtError, setDbtError] = useState<string | null>(null);
    // The DuckDB version this opened over when it opened for an outdated engine
    // (#410), which the app no longer runs on; null on a first run.
    const [upgradeFrom, setUpgradeFrom] = useState<string | null>(null);

    const refresh = useCallback(async () => {
        const list = await engineStatus();
        setEngines(list);
        setLoading(false);
        return list;
    }, []);

    useEffect(() => {
        void refresh().then(list => {
            const duck = list.find(e => e.id === 'duckdb');
            if (duck?.outdated) setUpgradeFrom(duck.version ?? '');
        });
        void dbtStatus().then(setDbtInstalled);
    }, [refresh]);

    const duckdb = engines.find(e => e.id === 'duckdb');
    const canContinue = Boolean(duckdb?.installed);

    // dbt Fusion is optional (only the dbt transform node needs it) so a failed
    // fetch is non-fatal: surfaced as a note, never blocking Continue.
    const installDbt = useCallback(async () => {
        setDbtBusy(true);
        setDbtError(null);
        try {
            await dbtInstall();
            setDbtInstalled(true);
        } catch (err) {
            setDbtError(String(err));
        } finally {
            setDbtBusy(false);
        }
    }, []);

    const install = async (id: string) => {
        setInstalling(id);
        setError(null);
        setProgress(p => ({ ...p, [id]: { phase: 'downloading', received: 0 } }));
        try {
            await engineInstall(id, p => {
                setProgress(prev => ({ ...prev, [id]: p }));
            });
            await refresh();
            // Provision dbt Fusion alongside DuckDB on the same first-run pass,
            // so the user sees it install as part of initialising the workspace.
            // Not on an upgrade, which asked for DuckDB alone.
            if (id === 'duckdb' && upgradeFrom === null && !dbtInstalled && !dbtBusy) {
                void installDbt();
            }
        } catch (err) {
            setError(String(err));
            setProgress(p => ({ ...p, [id]: { phase: 'failed', error: String(err) } }));
        } finally {
            setInstalling(null);
        }
    };

    return createPortal(
        <div className="modal-backdrop modal-backdrop-blocking">
            <div className="modal modal-engine">
                <div className="modal-header modal-workspace-header">
                    <div className="modal-workspace-mark">
                        <Workflow size={28} />
                    </div>
                    <div className="modal-workspace-titles">
                        <div className="modal-title">
                            {upgradeFrom !== null ? 'DuckDB upgrade' : 'Workspace initialisation'}
                        </div>
                        <div className="modal-subtitle">
                            {upgradeFrom !== null
                                ? `This version of Duckle runs on DuckDB ${duckdb?.target_version ?? ''}` +
                                  (upgradeFrom ? `, and this machine has ${upgradeFrom}` : '') +
                                  '. Upgrade it to continue: the new engine replaces the old one and ' +
                                  'fetches its extensions, and your workspaces and pipelines stay as they are.'
                                : 'First-run setup - Duckle stays a tiny download and fetches the DuckDB ' +
                                  'engine and dbt Fusion now. Sample pipelines and data are added to a ' +
                                  'new workspace so you have something to run right away.'}
                        </div>
                    </div>
                </div>

                <div className="modal-body modal-engine-body">
                    {loading ? (
                        <div className="engine-loading">
                            <Loader2 size={16} className="spin" /> Checking installed engines…
                        </div>
                    ) : (
                        <div className="engine-list">
                            {engines.map(e => (
                                <EngineRow
                                    key={e.id}
                                    engine={e}
                                    progress={progress[e.id]}
                                    installing={installing === e.id}
                                    disabled={installing !== null}
                                    onInstall={() => install(e.id)}
                                />
                            ))}
                            <DbtRow
                                installed={dbtInstalled}
                                busy={dbtBusy}
                                error={dbtError}
                                disabled={installing !== null}
                                onInstall={() => void installDbt()}
                            />
                        </div>
                    )}
                    {error ? <div className="modal-engine-error">{error}</div> : null}
                </div>

                <div className="modal-footer modal-engine-footer">
                    <span className="modal-engine-hint">
                        {canContinue
                            ? 'DuckDB ready.'
                            : upgradeFrom !== null
                              ? 'Upgrade DuckDB to continue.'
                              : 'DuckDB is required to run pipelines.'}
                    </span>
                    <button
                        type="button"
                        className="btn btn-primary"
                        onClick={onReady}
                        disabled={!canContinue}
                    >
                        Continue
                    </button>
                </div>
            </div>
        </div>,
        document.body,
    );
}

function EngineRow({
    engine,
    progress,
    installing,
    disabled,
    onInstall,
}: {
    engine: EngineStatus;
    progress?: InstallProgress;
    installing: boolean;
    disabled: boolean;
    onInstall: () => void;
}) {
    return (
        <div className="engine-row">
            <div className="engine-row-icon">
                <Cpu size={16} />
            </div>
            <div className="engine-row-info">
                <div className="engine-row-head">
                    <span className="engine-row-name">{engine.name}</span>
                    {engine.required ? (
                        <span className="engine-row-tag required">required</span>
                    ) : (
                        <span className="engine-row-tag">optional</span>
                    )}
                    {engine.version ? (
                        <span className="engine-row-ver">v{engine.version}</span>
                    ) : null}
                    {engine.outdated ? (
                        <span className="engine-row-tag">v{engine.target_version} available</span>
                    ) : null}
                </div>
                <div className="engine-row-desc">{engine.description}</div>
                {installing && progress ? (
                    <ProgressLine progress={progress} />
                ) : null}
            </div>
            <div className="engine-row-action">
                {engine.installed ? (
                    <span className="engine-row-installed">
                        <CheckCircle2 size={14} /> Installed
                    </span>
                ) : installing ? (
                    <span className="engine-row-installing">
                        <Loader2 size={13} className="spin" /> Installing…
                    </span>
                ) : engine.available ? (
                    <button
                        type="button"
                        className="btn btn-primary btn-sm"
                        onClick={onInstall}
                        disabled={disabled}
                    >
                        <Download size={13} /> {engine.outdated ? 'Upgrade' : 'Install'}
                    </button>
                ) : (
                    <span className="engine-row-unavailable">Not available</span>
                )}
            </div>
        </div>
    );
}

function DbtRow({
    installed,
    busy,
    error,
    disabled,
    onInstall,
}: {
    installed: boolean;
    busy: boolean;
    error: string | null;
    disabled: boolean;
    onInstall: () => void;
}) {
    return (
        <div className="engine-row">
            <div className="engine-row-icon">
                <Boxes size={16} />
            </div>
            <div className="engine-row-info">
                <div className="engine-row-head">
                    <span className="engine-row-name">dbt Fusion</span>
                    <span className="engine-row-tag">optional</span>
                </div>
                <div className="engine-row-desc">
                    Fast dbt engine for the dbt transform node. Installs alongside DuckDB; the
                    dbt-core fallback is fetched automatically if Fusion is unavailable.
                </div>
                {error ? (
                    <div className="engine-progress">
                        <div className="engine-progress-label">
                            {error} - you can retry, or install it later when you use a dbt node.
                        </div>
                    </div>
                ) : null}
            </div>
            <div className="engine-row-action">
                {installed ? (
                    <span className="engine-row-installed">
                        <CheckCircle2 size={14} /> Installed
                    </span>
                ) : busy ? (
                    <span className="engine-row-installing">
                        <Loader2 size={13} className="spin" /> Installing…
                    </span>
                ) : (
                    <button
                        type="button"
                        className="btn btn-primary btn-sm"
                        onClick={onInstall}
                        disabled={disabled}
                    >
                        <Download size={13} /> Install
                    </button>
                )}
            </div>
        </div>
    );
}

function ProgressLine({ progress }: { progress: InstallProgress }) {
    let label = '';
    let pct: number | null = null;
    switch (progress.phase) {
        case 'downloading': {
            const mb = (progress.received / 1_000_000).toFixed(1);
            if (progress.total) {
                pct = Math.round((progress.received / progress.total) * 100);
                const totalMb = (progress.total / 1_000_000).toFixed(1);
                label = `Downloading ${mb} / ${totalMb} MB`;
            } else {
                label = `Downloading ${mb} MB`;
            }
            break;
        }
        case 'extracting':
            label = 'Extracting…';
            break;
        case 'verifying':
            label = 'Verifying…';
            break;
        case 'installing_extension':
            // Pre-fetching the extensions Duckle uses so the first
            // Postgres / S3 / Excel touch is instant.
            label = `Installing extensions (${progress.index}/${progress.total}): ${progress.name}`;
            pct = Math.round((progress.index / progress.total) * 100);
            break;
        case 'downloading_model': {
            const mb = (progress.received / 1_000_000).toFixed(0);
            if (progress.total) {
                pct = Math.round((progress.received / progress.total) * 100);
                const totalMb = (progress.total / 1_000_000).toFixed(0);
                label = `Downloading model ${mb} / ${totalMb} MB`;
            } else {
                label = `Downloading model ${mb} MB`;
            }
            break;
        }
        case 'done':
            label = 'Ready';
            pct = 100;
            break;
        case 'failed':
            label = progress.error;
            break;
    }
    return (
        <div className="engine-progress">
            <div className="engine-progress-bar">
                <div
                    className="engine-progress-fill"
                    style={{ width: pct != null ? `${pct}%` : '40%' }}
                    data-indeterminate={pct == null}
                />
            </div>
            <div className="engine-progress-label">{label}</div>
        </div>
    );
}
