import { useEffect, useState } from 'react';
import { createPortal } from 'react-dom';
import { CheckCircle2, FolderOpen, Package, X } from 'lucide-react';
import { isTauri, tauriSavePath } from '../tauri-dialog';
import { buildBundle, type SecretsMode } from '../tauri-bridge';
import type { RepoItem } from '../repo-types';

type Props = {
    pipelineId: string;
    pipelineName: string;
    workspacePath: string | null;
    contexts: RepoItem[];
    onClose: () => void;
};

export default function BuildPipelineModal({
    pipelineId,
    pipelineName,
    workspacePath,
    contexts,
    onClose,
}: Props) {
    const [outFile, setOutFile] = useState('');
    const [contextName, setContextName] = useState('');
    const [secretsMode, setSecretsMode] = useState<SecretsMode>('env');
    const [passphrase, setPassphrase] = useState('');
    const [busy, setBusy] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [result, setResult] = useState<string | null>(null);

    useEffect(() => {
        const onKey = (e: KeyboardEvent) => {
            if (e.key === 'Escape' && !busy) onClose();
        };
        document.addEventListener('keydown', onKey);
        return () => document.removeEventListener('keydown', onKey);
    }, [onClose, busy]);

    const pickOutputFile = async () => {
        if (!isTauri()) return;
        // The built artifact is a single file named after the pipeline:
        // <name>.exe on Windows, <name> elsewhere. Use a save-file dialog.
        const isWin = navigator.userAgent.includes('Windows');
        const ext = isWin ? '.exe' : '';
        const defaultName = pipelineName.replace(/[^A-Za-z0-9._-]+/g, '_') + ext;
        const picked = await tauriSavePath({
            title: 'Save the built pipeline',
            defaultPath: defaultName,
            // An empty-extensions filter is invalid; off-Windows omit filters.
            filters: isWin ? [{ name: 'Executable', extensions: ['exe'] }] : undefined,
        });
        if (picked) setOutFile(picked);
    };

    const canBuild =
        !busy &&
        !!workspacePath &&
        outFile.trim().length > 0 &&
        (secretsMode !== 'passphrase' || passphrase.trim().length > 0);

    const handleBuild = async () => {
        if (!workspacePath) {
            setError('Open a workspace first.');
            return;
        }
        setBusy(true);
        setError(null);
        try {
            const path = await buildBundle(
                workspacePath,
                pipelineId,
                outFile.trim(),
                contextName || null,
                secretsMode,
                secretsMode === 'passphrase' ? passphrase : undefined,
            );
            setResult(path);
        } catch (e) {
            setError(String(e));
        } finally {
            setBusy(false);
        }
    };

    const handleBackdrop = (e: React.MouseEvent) => {
        if (e.target === e.currentTarget && !busy) onClose();
    };

    return createPortal(
        <div className="modal-backdrop" onClick={handleBackdrop}>
            <div className="modal">
                <div className="modal-header">
                    <div className="modal-title-row">
                        <Package size={16} className="modal-title-icon" />
                        <div>
                            <div className="modal-title">Build pipeline</div>
                            <div className="modal-subtitle">
                                Pipeline: <b>{pipelineName}</b>
                            </div>
                        </div>
                    </div>
                    <button type="button" className="modal-close" onClick={onClose} aria-label="Close" disabled={busy}>
                        <X size={16} />
                    </button>
                </div>

                {result ? (
                    <div className="modal-body">
                        <div className="modal-field" style={{ alignItems: 'center', textAlign: 'center' }}>
                            <CheckCircle2 size={28} color="var(--success)" />
                            <div className="modal-title" style={{ marginTop: 8 }}>Pipeline built</div>
                        </div>
                        <div className="modal-field">
                            <label className="modal-field-label">Built file</label>
                            <code className="modal-input" style={{ display: 'block', whiteSpace: 'normal', wordBreak: 'break-all' }}>
                                {result}
                            </code>
                        </div>
                        <div className="modal-tip">
                            <span>
                                This is one self-contained file. Copy this single file to your server and run
                                it or schedule it (cron / systemd / Task Scheduler). No install needed.
                            </span>
                        </div>
                        <div className="modal-footer">
                            <button type="button" className="btn btn-primary" onClick={onClose}>Close</button>
                        </div>
                    </div>
                ) : (
                    <div className="modal-body">
                        <div className="modal-field">
                            <label className="modal-field-label">Output file</label>
                            <div className="schedule-watch-row">
                                <input
                                    type="text"
                                    className="modal-input"
                                    value={outFile}
                                    onChange={e => setOutFile(e.target.value)}
                                    placeholder="Choose where to save the single file"
                                    spellCheck={false}
                                />
                                <button type="button" className="btn btn-secondary" onClick={pickOutputFile} disabled={busy}>
                                    <FolderOpen size={13} /> Browse
                                </button>
                            </div>
                        </div>

                        <div className="modal-field">
                            <label className="modal-field-label">Context</label>
                            <select
                                className="modal-input modal-select"
                                value={contextName}
                                onChange={e => setContextName(e.target.value)}
                                disabled={busy}
                            >
                                <option value="">No context</option>
                                {contexts.map(c => (
                                    <option key={c.id} value={c.name}>{c.name}</option>
                                ))}
                            </select>
                        </div>

                        <div className="modal-field">
                            <label className="modal-field-label">Secrets</label>
                            <div className="schedule-mode-toggle">
                                <button
                                    type="button"
                                    className={'schedule-mode-button' + (secretsMode === 'env' ? ' is-active' : '')}
                                    onClick={() => setSecretsMode('env')}
                                    disabled={busy}
                                >
                                    Environment
                                </button>
                                <button
                                    type="button"
                                    className={'schedule-mode-button' + (secretsMode === 'passphrase' ? ' is-active' : '')}
                                    onClick={() => setSecretsMode('passphrase')}
                                    disabled={busy}
                                >
                                    Passphrase
                                </button>
                            </div>
                        </div>

                        {secretsMode === 'passphrase' ? (
                            <div className="modal-field">
                                <label className="modal-field-label">Passphrase</label>
                                <input
                                    type="password"
                                    className="modal-input"
                                    value={passphrase}
                                    onChange={e => setPassphrase(e.target.value)}
                                    placeholder="Used to encrypt the bundled secrets"
                                    spellCheck={false}
                                />
                                <div className="modal-tip">
                                    <span>
                                        Secrets are encrypted into secrets.enc. Set DUCKLE_BUNDLE_PASSPHRASE in the
                                        server environment with this same value before running. The passphrase is
                                        never written into the file.
                                    </span>
                                </div>
                            </div>
                        ) : (
                            <div className="modal-tip">
                                <span>
                                    Environment mode: set the secret env vars on the server, or place a
                                    secrets.env next to the file before running.
                                </span>
                            </div>
                        )}

                        {error ? <div className="modal-error">{error}</div> : null}

                        <div className="modal-footer">
                            <button type="button" className="btn btn-secondary" onClick={onClose} disabled={busy}>Cancel</button>
                            <button type="button" className="btn btn-primary" onClick={handleBuild} disabled={!canBuild}>
                                <Package size={13} />
                                {busy ? 'Building…' : 'Build'}
                            </button>
                        </div>
                    </div>
                )}
            </div>
        </div>,
        document.body,
    );
}
