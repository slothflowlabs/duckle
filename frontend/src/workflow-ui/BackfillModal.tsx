import { useCallback, useEffect, useState } from 'react';
import { createPortal } from 'react-dom';
import { History, RotateCcw, Save, Trash2, X } from 'lucide-react';
import { isTauri } from '../tauri-dialog';
import {
    watermarkClear,
    watermarkList,
    watermarkSet,
    type WatermarkEntry,
} from '../tauri-bridge';

type Props = {
    pipelineName: string;
    workspacePath: string | null;
    onClose: () => void;
};

// Backfill: inspect and reset the saved state that xf.incremental (a
// high-water mark) and src.ducklake.changes (a CDC snapshot id) advance only
// on a fully successful run. Editing it replays from an earlier point; clearing
// it forces a full reload on the next run. State only appears here after a node
// has run at least once and written state.
export default function BackfillModal({ pipelineName, workspacePath, onClose }: Props) {
    const [entries, setEntries] = useState<WatermarkEntry[]>([]);
    // node_id -> edited value (controlled inputs)
    const [drafts, setDrafts] = useState<Record<string, string>>({});
    const [loading, setLoading] = useState(true);
    const [busy, setBusy] = useState<string | null>(null);
    const [error, setError] = useState<string | null>(null);

    const reload = useCallback(async () => {
        if (!workspacePath) {
            setEntries([]);
            setLoading(false);
            return;
        }
        setLoading(true);
        const list = await watermarkList(workspacePath, pipelineName);
        setEntries(list);
        setDrafts(Object.fromEntries(list.map(e => [e.node_id, e.value])));
        setLoading(false);
    }, [workspacePath, pipelineName]);

    useEffect(() => {
        void reload();
    }, [reload]);

    const handleBackdrop = (e: React.MouseEvent) => {
        if (e.target === e.currentTarget) onClose();
    };

    const save = async (entry: WatermarkEntry) => {
        if (!workspacePath) return;
        setBusy(entry.node_id);
        setError(null);
        try {
            await watermarkSet(
                workspacePath,
                pipelineName,
                entry.node_id,
                entry.kind,
                drafts[entry.node_id] ?? entry.value,
                entry.value_type,
            );
            await reload();
        } catch (e) {
            setError(String(e));
        } finally {
            setBusy(null);
        }
    };

    const clear = async (entry: WatermarkEntry) => {
        if (!workspacePath) return;
        setBusy(entry.node_id);
        setError(null);
        try {
            await watermarkClear(workspacePath, pipelineName, entry.node_id);
            await reload();
        } catch (e) {
            setError(String(e));
        } finally {
            setBusy(null);
        }
    };

    return createPortal(
        <div className="modal-backdrop" onClick={handleBackdrop}>
            <div className="modal modal-backfill">
                <div className="modal-header">
                    <div className="modal-title-row">
                        <History size={16} className="modal-title-icon" />
                        <div>
                            <div className="modal-title">Backfill</div>
                            <div className="modal-subtitle">
                                Pipeline: <b>{pipelineName}</b>
                            </div>
                        </div>
                    </div>
                    <button
                        type="button"
                        className="modal-close"
                        onClick={onClose}
                        aria-label="Close"
                    >
                        <X size={16} />
                    </button>
                </div>

                <div className="modal-body modal-backfill-body">
                    <p className="backfill-intro">
                        Saved incremental watermarks and CDC snapshots. Edit a value and Save to
                        replay from that point on the next run, or Clear to force a full reload.
                        State appears here after a node has run once.
                    </p>

                    {error ? <div className="backfill-error">{error}</div> : null}

                    {!isTauri() ? (
                        <div className="backfill-empty">Backfill is available in the desktop app.</div>
                    ) : loading ? (
                        <div className="backfill-empty">Loading…</div>
                    ) : entries.length === 0 ? (
                        <div className="backfill-empty">
                            No saved state yet. Run a pipeline with an Incremental Load or DuckLake
                            change-feed node to populate it.
                        </div>
                    ) : (
                        <table className="backfill-table">
                            <thead>
                                <tr>
                                    <th>Node</th>
                                    <th>Kind</th>
                                    <th>Value</th>
                                    <th></th>
                                </tr>
                            </thead>
                            <tbody>
                                {entries.map(e => (
                                    <tr key={e.node_id}>
                                        <td className="backfill-node">{e.node_id}</td>
                                        <td>
                                            <span className="backfill-kind">{e.kind}</span>
                                            {e.value_type ? (
                                                <span className="backfill-type">{e.value_type}</span>
                                            ) : null}
                                        </td>
                                        <td>
                                            <input
                                                className="modal-input backfill-value"
                                                disabled={e.editable === false}
                                                title={
                                                    e.editable === false
                                                        ? `A ${e.kind} node does not resume from a hand-written value. Clear it to start that node over.`
                                                        : undefined
                                                }
                                                value={drafts[e.node_id] ?? ''}
                                                onChange={ev =>
                                                    setDrafts(d => ({
                                                        ...d,
                                                        [e.node_id]: ev.target.value,
                                                    }))
                                                }
                                                spellCheck={false}
                                            />
                                        </td>
                                        <td className="backfill-actions">
                                            <button
                                                type="button"
                                                className="backfill-btn"
                                                disabled={
                                                    busy === e.node_id ||
                                                    e.editable === false ||
                                                    (drafts[e.node_id] ?? e.value) === e.value
                                                }
                                                onClick={() => save(e)}
                                                title="Save watermark"
                                            >
                                                <Save size={13} /> Save
                                            </button>
                                            <button
                                                type="button"
                                                className="backfill-btn backfill-btn-danger"
                                                disabled={busy === e.node_id}
                                                onClick={() => clear(e)}
                                                title="Clear state (full reload next run)"
                                            >
                                                <Trash2 size={13} /> Clear
                                            </button>
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    )}

                    <button type="button" className="backfill-refresh" onClick={() => void reload()}>
                        <RotateCcw size={13} /> Refresh
                    </button>
                </div>
            </div>
        </div>,
        document.body,
    );
}
