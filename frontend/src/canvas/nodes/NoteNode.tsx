import { useContext, useEffect, useRef, useState } from 'react';
import type { Node, NodeProps } from '@xyflow/react';
import type { NoteData } from '../../pipeline-types';
import { NoteContext } from '../note-context';

/**
 * A note on the canvas. Double-click to write in it; clicking away keeps what was
 * written and Escape throws it away.
 */
export default function NoteNode({ id, data, selected }: NodeProps<Node<NoteData>>) {
    const { editing, setEditing, commit } = useContext(NoteContext);
    const isEditing = editing === id;
    const [draft, setDraft] = useState(data.text);
    const input = useRef<HTMLTextAreaElement>(null);
    // Escape unmounts the field, and the blur that may follow must not save it.
    const cancelled = useRef(false);

    useEffect(() => {
        if (!isEditing) return;
        cancelled.current = false;
        setDraft(data.text);
        // A new note stays hidden until the canvas has measured it, and a hidden
        // field cannot take the focus, so keep trying for a few frames.
        let frame = 0;
        let tries = 0;
        const focus = () => {
            const el = input.current;
            if (!el) return;
            el.focus();
            if (document.activeElement === el) el.setSelectionRange(el.value.length, el.value.length);
            else if (++tries < 30) frame = requestAnimationFrame(focus);
        };
        focus();
        return () => cancelAnimationFrame(frame);
        // Only on entering the field: a re-render while typing must not reset it.
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [isEditing]);

    const finish = () => {
        if (!cancelled.current && draft !== data.text) commit(id, draft);
        setEditing(null);
    };

    return (
        <div className={'canvas-note' + (selected ? ' is-selected' : '')} onDoubleClick={() => setEditing(id)}>
            {isEditing ? (
                <textarea
                    ref={input}
                    className="canvas-note-input nodrag nowheel"
                    value={draft}
                    placeholder="Write a note"
                    onChange={e => setDraft(e.target.value)}
                    onBlur={finish}
                    onKeyDown={e => {
                        if (e.key !== 'Escape') return;
                        e.preventDefault();
                        cancelled.current = true;
                        setEditing(null);
                    }}
                />
            ) : (
                <div className="canvas-note-text">
                    {data.text || <span className="canvas-note-placeholder">Double-click to write a note</span>}
                </div>
            )}
        </div>
    );
}
