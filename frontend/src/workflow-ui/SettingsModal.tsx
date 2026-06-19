import { useEffect, useState } from 'react';
import { createPortal } from 'react-dom';
import { X, Loader2, Check } from 'lucide-react';
import { settingsGetProxy, settingsSetProxy } from '../tauri-bridge';

/**
 * App settings. Currently a single HTTP/HTTPS proxy field, persisted per
 * workspace to .duckle/settings.json and applied to the engine immediately, so
 * a user on a locked-down corporate machine can route REST / cloud connectors
 * and the updater through a proxy without setting a system env var (issue #80).
 */
export function SettingsModal({
    workspace,
    onClose,
}: {
    workspace: string | null;
    onClose: () => void;
}) {
    const [proxy, setProxy] = useState('');
    const [loaded, setLoaded] = useState(false);
    const [saving, setSaving] = useState(false);
    const [saved, setSaved] = useState(false);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        let alive = true;
        if (!workspace) {
            setLoaded(true);
            return;
        }
        settingsGetProxy(workspace)
            .then(v => {
                if (alive) {
                    setProxy(v ?? '');
                    setLoaded(true);
                }
            })
            .catch(e => {
                if (alive) {
                    setError(String(e));
                    setLoaded(true);
                }
            });
        return () => {
            alive = false;
        };
    }, [workspace]);

    const save = async () => {
        if (!workspace) return;
        setSaving(true);
        setError(null);
        setSaved(false);
        try {
            await settingsSetProxy(workspace, proxy.trim() || null);
            setSaved(true);
            setTimeout(() => setSaved(false), 1500);
        } catch (e) {
            setError(String(e));
        } finally {
            setSaving(false);
        }
    };

    const handleBackdrop = (e: React.MouseEvent) => {
        if (e.target === e.currentTarget) onClose();
    };
    const btn: React.CSSProperties = {
        padding: '7px 14px',
        borderRadius: 8,
        border: '1px solid var(--border-2, #2a2a2a)',
        background: 'transparent',
        color: 'inherit',
        cursor: 'pointer',
        fontWeight: 600,
        display: 'inline-flex',
        alignItems: 'center',
        gap: 6,
    };
    const primary: React.CSSProperties = {
        ...btn,
        background: 'var(--accent, #ff7a45)',
        borderColor: 'var(--accent, #ff7a45)',
        color: '#0a0a0a',
    };

    return createPortal(
        <div className="modal-backdrop" onClick={handleBackdrop}>
            <div
                className="modal"
                role="dialog"
                aria-modal="true"
                aria-label="Settings"
                style={{ maxWidth: 480 }}
            >
                <div className="modal-header">
                    <div className="modal-title">Settings</div>
                    <button type="button" className="modal-close" onClick={onClose} aria-label="Close">
                        <X size={16} />
                    </button>
                </div>
                <div className="modal-body">
                    <label
                        htmlFor="settings-proxy"
                        style={{ display: 'block', fontWeight: 600, marginBottom: 6 }}
                    >
                        HTTP / HTTPS proxy
                    </label>
                    <p style={{ marginTop: 0, marginBottom: 8, fontSize: 12, opacity: 0.7 }}>
                        Routes REST and cloud-API connectors and the in-app updater through a proxy, so
                        Duckle works behind a corporate proxy without setting a system environment
                        variable. Leave empty for a direct connection.
                    </p>
                    <input
                        id="settings-proxy"
                        type="text"
                        value={proxy}
                        onChange={e => setProxy(e.target.value)}
                        placeholder="http://user:pass@proxy.company.com:8080"
                        disabled={!loaded || !workspace}
                        spellCheck={false}
                        autoComplete="off"
                        style={{
                            width: '100%',
                            padding: '8px 10px',
                            borderRadius: 8,
                            border: '1px solid var(--border-2, #2a2a2a)',
                            background: 'var(--bg-1, #14161c)',
                            color: 'inherit',
                            boxSizing: 'border-box',
                        }}
                    />
                    {!workspace ? (
                        <p style={{ fontSize: 12, color: 'var(--danger, #ff4d6d)', marginBottom: 0 }}>
                            Open a workspace first to save settings.
                        </p>
                    ) : null}
                    {error ? (
                        <p style={{ fontSize: 12, color: 'var(--danger, #ff4d6d)', marginBottom: 0 }}>
                            {error}
                        </p>
                    ) : null}
                </div>
                <div className="modal-footer" style={{ display: 'flex', justifyContent: 'flex-end', gap: 8 }}>
                    <button type="button" style={btn} onClick={onClose}>
                        Close
                    </button>
                    <button type="button" style={primary} onClick={save} disabled={saving || !workspace}>
                        {saving ? <Loader2 size={14} className="spin" /> : saved ? <Check size={14} /> : null}
                        {saved ? 'Saved' : 'Save'}
                    </button>
                </div>
            </div>
        </div>,
        document.body
    );
}
