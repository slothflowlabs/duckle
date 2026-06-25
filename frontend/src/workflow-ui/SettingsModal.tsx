import { useEffect, useState } from 'react';
import { createPortal } from 'react-dom';
import { X, Loader2, Check } from 'lucide-react';
import {
    settingsGetProxy,
    settingsSetProxy,
    settingsGetAi,
    settingsSetAi,
    settingsGetMemoryLimit,
    settingsSetMemoryLimit,
    settingsGetContextFile,
    settingsSetContextFile,
} from '../tauri-bridge';
import { loadPersisted, savePersisted } from '../persistence';

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
    // #92: external OpenAI-compatible AI endpoint for the Duckie assistant.
    const [aiBaseUrl, setAiBaseUrl] = useState('');
    const [aiModel, setAiModel] = useState('');
    const [aiKey, setAiKey] = useState('');
    // #102: per-workspace total memory cap in MB (empty = engine default).
    const [memLimit, setMemLimit] = useState('');
    // Global context file: a key/value file auto-merged into the global context.
    const [contextFile, setContextFile] = useState('');
    // Local UI pref: show/hide the top-bar Dives button.
    const [showDives, setShowDives] = useState(() => !loadPersisted('hideDivesButton', false));
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
        Promise.all([
            settingsGetProxy(workspace),
            settingsGetAi(workspace),
            settingsGetMemoryLimit(workspace),
            settingsGetContextFile(workspace),
        ])
            .then(([p, ai, mem, ic]) => {
                if (!alive) return;
                setProxy(p ?? '');
                setAiBaseUrl(ai.baseUrl ?? '');
                setAiModel(ai.model ?? '');
                setAiKey(ai.apiKey ?? '');
                setMemLimit(mem != null ? String(mem) : '');
                setContextFile(ic ?? '');
                setLoaded(true);
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
            await settingsSetAi(workspace, {
                baseUrl: aiBaseUrl.trim() || null,
                model: aiModel.trim() || null,
                apiKey: aiKey.trim() || null,
            });
            const mb = parseInt(memLimit.trim(), 10);
            await settingsSetMemoryLimit(workspace, Number.isFinite(mb) && mb > 0 ? mb : null);
            await settingsSetContextFile(workspace, contextFile.trim() || null);
            setSaved(true);
            setTimeout(() => setSaved(false), 1500);
        } catch (e) {
            setError(String(e));
        } finally {
            setSaving(false);
        }
    };

    // Local UI pref - applies immediately (no Save), broadcast so App re-reads.
    const toggleDives = (next: boolean) => {
        setShowDives(next);
        savePersisted('hideDivesButton', !next);
        window.dispatchEvent(new Event('duckle:dives-visibility'));
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
    const aiInput: React.CSSProperties = {
        width: '100%',
        padding: '8px 10px',
        borderRadius: 8,
        border: '1px solid var(--border-2, #2a2a2a)',
        background: 'var(--bg-1, #14161c)',
        color: 'inherit',
        boxSizing: 'border-box',
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
                    <div style={{ borderTop: '1px solid var(--border-2, #2a2a2a)', margin: '16px 0 12px' }} />
                    <label htmlFor="settings-mem" style={{ display: 'block', fontWeight: 600, marginBottom: 6 }}>
                        Memory limit (MB)
                    </label>
                    <p style={{ marginTop: 0, marginBottom: 8, fontSize: 12, opacity: 0.7 }}>
                        Caps total RAM for every run in this workspace (sets DuckDB's memory_limit for
                        both batched and per-stage execution). Leave empty for the engine default
                        (about 80% of system RAM).
                    </p>
                    <input
                        id="settings-mem"
                        type="number"
                        min={0}
                        value={memLimit}
                        onChange={e => setMemLimit(e.target.value)}
                        placeholder="e.g. 4096"
                        disabled={!loaded || !workspace}
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
                    <div style={{ borderTop: '1px solid var(--border-2, #2a2a2a)', margin: '16px 0 12px' }} />
                    <label htmlFor="settings-context-file" style={{ display: 'block', fontWeight: 600, marginBottom: 6 }}>
                        Global context file
                    </label>
                    <p style={{ marginTop: 0, marginBottom: 8, fontSize: 12, opacity: 0.7 }}>
                        Auto-load context variables from a key/value file before every run, so{' '}
                        <code>{'${KEY}'}</code> resolves everywhere without wiring a node. Supports .env /
                        .properties (KEY=VALUE), .csv (key,value) and .json. A relative path is resolved
                        against the workspace root.
                    </p>
                    <input
                        id="settings-context-file"
                        type="text"
                        value={contextFile}
                        onChange={e => setContextFile(e.target.value)}
                        placeholder="config/context.env  (or an absolute path)"
                        disabled={!loaded || !workspace}
                        spellCheck={false}
                        autoComplete="off"
                        style={aiInput}
                    />
                    <div style={{ borderTop: '1px solid var(--border-2, #2a2a2a)', margin: '16px 0 12px' }} />
                    <label style={{ display: 'block', fontWeight: 600, marginBottom: 6 }}>
                        AI assistant endpoint
                    </label>
                    <p style={{ marginTop: 0, marginBottom: 8, fontSize: 12, opacity: 0.7 }}>
                        Point Duckie at an external OpenAI-compatible API (OpenAI, Ollama, LM Studio,
                        vLLM, ...) instead of the bundled local model. Leave the base URL empty to use
                        the local Qwen model.
                    </p>
                    <input
                        type="text"
                        value={aiBaseUrl}
                        onChange={e => setAiBaseUrl(e.target.value)}
                        placeholder="Base URL, e.g. https://api.openai.com"
                        disabled={!loaded || !workspace}
                        spellCheck={false}
                        autoComplete="off"
                        style={aiInput}
                    />
                    <input
                        type="text"
                        value={aiModel}
                        onChange={e => setAiModel(e.target.value)}
                        placeholder="Model, e.g. gpt-4o-mini"
                        disabled={!loaded || !workspace}
                        spellCheck={false}
                        autoComplete="off"
                        style={{ ...aiInput, marginTop: 8 }}
                    />
                    <input
                        type="password"
                        value={aiKey}
                        onChange={e => setAiKey(e.target.value)}
                        placeholder="API key (sent as a Bearer token)"
                        disabled={!loaded || !workspace}
                        spellCheck={false}
                        autoComplete="off"
                        style={{ ...aiInput, marginTop: 8 }}
                    />
                    <div style={{ borderTop: '1px solid var(--border-2, #2a2a2a)', margin: '16px 0 12px' }} />
                    <label style={{ display: 'block', fontWeight: 600, marginBottom: 6 }}>Toolbar</label>
                    <label style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 13, cursor: 'pointer', marginBottom: 4 }}>
                        <input type="checkbox" checked={showDives} onChange={e => toggleDives(e.target.checked)} />
                        Show the Dives button (live data views &amp; dashboards) in the toolbar
                    </label>
                    <div style={{ borderTop: '1px solid var(--border-2, #2a2a2a)', margin: '16px 0 12px' }} />
                    <label style={{ display: 'block', fontWeight: 600, marginBottom: 6 }}>Guided tour</label>
                    <p style={{ marginTop: 0, marginBottom: 8, fontSize: 12, opacity: 0.7 }}>
                        Replay the first-run walkthrough of the palette, canvas, properties, Run and the
                        web dashboard.
                    </p>
                    <button
                        type="button"
                        style={btn}
                        onClick={() => {
                            onClose();
                            setTimeout(() => window.dispatchEvent(new Event('duckle:start-tour')), 250);
                        }}
                    >
                        Replay guided tour
                    </button>
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
