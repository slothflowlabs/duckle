import { useEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { X } from 'lucide-react';

type Props = {
    /** The link, as people read it: "Read orders -> Keep paid". */
    linkLabel: string;
    /** The breakpoint's condition now, or empty when the link has none. */
    condition: string;
    /** Save a condition; an empty one takes the breakpoint off. */
    onSave: (condition: string) => void;
    onCancel: () => void;
};

/**
 * Set the breakpoint on a link: a SQL condition on the rows that go down it. A
 * run started in the editor stops after the link's source step when any row
 * matches, and shows those rows; scheduled and deployed runs ignore it.
 */
export default function BreakpointModal({ linkLabel, condition, onSave, onCancel }: Props) {
    const [draft, setDraft] = useState(condition);
    const ref = useRef<HTMLTextAreaElement>(null);

    useEffect(() => {
        setTimeout(() => ref.current?.focus(), 30);
        const onKey = (e: KeyboardEvent) => {
            if (e.key === 'Escape') onCancel();
        };
        document.addEventListener('keydown', onKey);
        return () => document.removeEventListener('keydown', onKey);
    }, [onCancel]);

    return createPortal(
        <div
            className="modal-backdrop"
            role="dialog"
            aria-modal="true"
            onClick={e => {
                if (e.target === e.currentTarget) onCancel();
            }}
        >
            <div className="modal modal-edge-editor">
                <div className="modal-header">
                    <div className="modal-title">Breakpoint on {linkLabel}</div>
                    <button type="button" className="modal-close" onClick={onCancel} aria-label="Close">
                        <X size={16} />
                    </button>
                </div>
                <div className="modal-body">
                    <div className="modal-field">
                        <label className="modal-field-label">Stop when a row matches (SQL condition)</label>
                        <textarea
                            ref={ref}
                            className="modal-input modal-input-path"
                            value={draft}
                            placeholder="amount < 0 OR customer_id IS NULL"
                            onChange={e => setDraft(e.target.value)}
                            onKeyDown={e => {
                                if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
                                    e.preventDefault();
                                    onSave(draft);
                                }
                            }}
                            rows={3}
                            spellCheck={false}
                        />
                        <div className="modal-field-hint">
                            A run you start here stops after the step this link leaves when any row going
                            down it matches, and shows the rows that did. Nothing past the link runs.
                            Scheduled and deployed runs ignore breakpoints.
                        </div>
                    </div>
                </div>
                <div className="modal-footer">
                    {condition ? (
                        <button type="button" className="btn btn-secondary" onClick={() => onSave('')}>
                            Remove breakpoint
                        </button>
                    ) : null}
                    <button type="button" className="btn btn-secondary" onClick={onCancel}>
                        Cancel
                    </button>
                    <button type="button" className="btn btn-primary" onClick={() => onSave(draft)}>
                        Save
                    </button>
                </div>
            </div>
        </div>,
        document.body,
    );
}
