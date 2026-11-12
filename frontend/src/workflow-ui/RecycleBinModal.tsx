import { useEffect, useState } from 'react';
import { createPortal } from 'react-dom';
import { RotateCcw, Trash2, X } from 'lucide-react';
import { batchesOf, describeBatch, type TrashEntry, type TrashItem } from '../recycle-bin';
import { listTrash, purgeFromTrash, restoreFromTrash } from '../workspace';

type Props = {
    workspace: string;
    repo: TrashItem[];
    onClose: () => void;
    /** After a restore: the app re-reads the workspace from disk. */
    onRestored: () => void;
};

function deletedWhen(iso: string): string {
    const d = new Date(iso);
    return Number.isNaN(d.getTime()) ? iso : d.toLocaleString();
}

/** The recycle bin: what was deleted from the project tree, to put back or to
 * delete for good. Deleting for good asks twice, because it cannot be undone. */
export default function RecycleBinModal({ workspace, repo, onClose, onRestored }: Props) {
    const [entries, setEntries] = useState<TrashEntry[] | null>(null);
    const [error, setError] = useState<string | null>(null);
    const [busy, setBusy] = useState(false);
    // The batch (or '*' for the whole bin) whose permanent delete was asked for once.
    const [armed, setArmed] = useState<string | null>(null);

    const reload = () => {
        listTrash(workspace)
            .then(setEntries)
            .catch(e => setError(e instanceof Error ? e.message : String(e)));
    };
    useEffect(reload, [workspace]);
    useEffect(() => {
        const onKey = (e: KeyboardEvent) => {
            if (e.key === 'Escape') onClose();
        };
        document.addEventListener('keydown', onKey);
        return () => document.removeEventListener('keydown', onKey);
    }, [onClose]);

    const act = async (work: () => Promise<void>, after: () => void) => {
        setBusy(true);
        setError(null);
        try {
            await work();
            setArmed(null);
            after();
        } catch (e) {
            setError(e instanceof Error ? e.message : String(e));
        } finally {
            setBusy(false);
        }
    };

    const batches = entries ? batchesOf(entries) : [];

    return createPortal(
        <div
            className="modal-backdrop"
            role="dialog"
            aria-modal="true"
            onClick={e => {
                if (e.target === e.currentTarget) onClose();
            }}
        >
            <div className="modal recycle-bin">
                <div className="modal-header">
                    <div className="modal-title">Recycle bin</div>
                    <button type="button" className="modal-close" onClick={onClose} aria-label="Close">
                        <X size={16} />
                    </button>
                </div>
                <div className="modal-body">
                    {error ? <div className="recycle-bin-error">{error}</div> : null}
                    {entries === null ? (
                        <div className="modal-field-hint">Loading…</div>
                    ) : batches.length === 0 ? (
                        <div className="modal-field-hint">
                            The bin is empty. A pipeline, folder, connection or other item you delete
                            comes here, and can be put back.
                        </div>
                    ) : (
                        <ul className="recycle-bin-list">
                            {batches.map(b => (
                                <li key={b.batch} className="recycle-bin-row">
                                    <div className="recycle-bin-what">
                                        <div className="recycle-bin-name">{describeBatch(b)}</div>
                                        <div className="recycle-bin-when">Deleted {deletedWhen(b.deletedAt)}</div>
                                    </div>
                                    <button
                                        type="button"
                                        className="btn btn-secondary"
                                        disabled={busy}
                                        onClick={() =>
                                            void act(() => restoreFromTrash(workspace, b.entries, repo), onRestored)
                                        }
                                    >
                                        <RotateCcw size={13} /> Restore
                                    </button>
                                    <button
                                        type="button"
                                        className={armed === b.batch ? 'btn is-danger' : 'btn btn-secondary'}
                                        disabled={busy}
                                        onClick={() =>
                                            armed === b.batch
                                                ? void act(() => purgeFromTrash(workspace, b.entries), reload)
                                                : setArmed(b.batch)
                                        }
                                    >
                                        <Trash2 size={13} />{' '}
                                        {armed === b.batch ? 'Delete for good: cannot be undone' : 'Delete forever'}
                                    </button>
                                </li>
                            ))}
                        </ul>
                    )}
                </div>
                <div className="modal-footer">
                    {batches.length > 0 ? (
                        <button
                            type="button"
                            className={armed === '*' ? 'btn is-danger' : 'btn btn-secondary'}
                            disabled={busy}
                            onClick={() =>
                                armed === '*'
                                    ? void act(() => purgeFromTrash(workspace, entries ?? []), reload)
                                    : setArmed('*')
                            }
                        >
                            {armed === '*' ? 'Empty the bin: cannot be undone' : 'Empty bin'}
                        </button>
                    ) : null}
                    <button type="button" className="btn btn-primary" onClick={onClose}>
                        Close
                    </button>
                </div>
            </div>
        </div>,
        document.body,
    );
}
