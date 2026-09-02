/**
 * #314: a SQL textarea that suggests what could come next.
 *
 * Deliberately a plain textarea with a list underneath rather than an editor
 * component: the field already works, people already have habits in it, and
 * swapping it for a code editor to add completion would change everything about
 * typing in order to change one thing.
 *
 * The suggestions come from the engine - upstream columns with their types, the
 * relations the node can read, the pipeline's parameters, DuckDB's functions -
 * so the list is what the SQL will actually bind against rather than a guess
 * from the text.
 */
import { useCallback, useEffect, useRef, useState } from 'react';
import { completeNodeSql, type SqlCompletion } from '../../tauri-bridge';
import { sqlContext } from './sql-context';

type Props = {
    value: string;
    onChange: (v: string) => void;
    placeholder?: string;
    rows?: number;
    mono?: boolean;
};

/** Long enough that typing a word does not ask five times, short enough to feel immediate. */
const DEBOUNCE_MS = 120;

export default function SqlField({ value, onChange, placeholder, rows, mono }: Props) {
    const ref = useRef<HTMLTextAreaElement | null>(null);
    const [items, setItems] = useState<SqlCompletion[]>([]);
    const [active, setActive] = useState(0);
    const [open, setOpen] = useState(false);
    // Bumped on every edit; a reply for an older bump is dropped, so a slow
    // answer cannot overwrite the list for what is now being typed.
    const asked = useRef(0);
    const timer = useRef<ReturnType<typeof setTimeout> | null>(null);

    const ask = useCallback((cursor: number, now = false) => {
        // Debounced: typing a word should ask once, not once per letter. The
        // engine side is cheap, but the round trip is not free and a list that
        // flickers per keystroke is harder to read than one that settles.
        if (timer.current) clearTimeout(timer.current);
        const fire = () => {
            const context = sqlContext();
            if (!context) return;
            const mine = ++asked.current;
            void completeNodeSql(
                context.nodes,
                context.edges,
                context.nodeId,
                context.inputs,
                cursor,
            ).then(got => {
                // A reply for an earlier keystroke is discarded rather than
                // shown: it describes text that is no longer there.
                if (mine !== asked.current) return;
                setItems(got);
                setActive(0);
                setOpen(got.length > 0);
            });
        };
        // Ctrl-Space is a deliberate request and should not wait.
        if (now) fire();
        else timer.current = setTimeout(fire, DEBOUNCE_MS);
    }, []);

    // A pending request must not fire after the field is gone.
    useEffect(() => () => {
        if (timer.current) clearTimeout(timer.current);
    }, []);

    useEffect(() => {
        if (!open) return;
        const close = () => setOpen(false);
        // Clicking anywhere else means the author moved on.
        window.addEventListener('mousedown', close);
        return () => window.removeEventListener('mousedown', close);
    }, [open]);

    const insert = (choice: SqlCompletion) => {
        const el = ref.current;
        if (!el) return;
        const cursor = el.selectionStart ?? value.length;
        // Replace the word being typed, not the whole line: the author has
        // already written part of it and expects that part to be used.
        const head = value.slice(0, cursor);
        const start = head.length - (head.match(/[A-Za-z0-9_${]*$/)?.[0].length ?? 0);
        const next = value.slice(0, start) + choice.text + value.slice(cursor);
        onChange(next);
        setOpen(false);
        // Put the caret after what was inserted, so typing continues where the
        // author would expect rather than at the end of the statement.
        const at = start + choice.text.length;
        requestAnimationFrame(() => {
            el.focus();
            el.setSelectionRange(at, at);
        });
    };

    return (
        <div className="sql-field">
            <textarea
                ref={ref}
                className={'field-input field-textarea' + (mono ? ' field-mono' : '')}
                value={value ?? ''}
                placeholder={placeholder}
                rows={rows ?? 6}
                spellCheck={false}
                onChange={e => {
                    onChange(e.target.value);
                    ask(e.target.selectionStart ?? e.target.value.length);
                }}
                onKeyDown={e => {
                    // Ctrl-Space asks even when nothing was typed - the way
                    // every editor does it, and the only way to see what is
                    // available at a fresh position.
                    if (e.key === ' ' && (e.ctrlKey || e.metaKey)) {
                        e.preventDefault();
                        const el = e.currentTarget;
                        ask(el.selectionStart ?? el.value.length, true);
                        return;
                    }
                    if (!open || items.length === 0) return;
                    if (e.key === 'ArrowDown') {
                        e.preventDefault();
                        setActive(a => (a + 1) % items.length);
                    } else if (e.key === 'ArrowUp') {
                        e.preventDefault();
                        setActive(a => (a - 1 + items.length) % items.length);
                    } else if (e.key === 'Enter' || e.key === 'Tab') {
                        // Only when the list is open: Enter still means newline
                        // the rest of the time, because this is a SQL editor
                        // before it is an autocomplete.
                        e.preventDefault();
                        insert(items[active]);
                    } else if (e.key === 'Escape') {
                        e.preventDefault();
                        setOpen(false);
                    }
                }}
                onBlur={() => setOpen(false)}
            />
            {open && items.length > 0 ? (
                <ul className="sql-completions" role="listbox">
                    {items.map((c, i) => (
                        <li
                            key={c.text + i}
                            role="option"
                            aria-selected={i === active}
                            className={i === active ? 'is-active' : undefined}
                            // mousedown, not click: blur fires first on click and
                            // the list would be gone before the choice landed.
                            onMouseDown={e => {
                                e.preventDefault();
                                insert(c);
                            }}
                            onMouseEnter={() => setActive(i)}
                        >
                            <span className={'sql-completion-kind kind-' + c.kind}>{c.kind[0]}</span>
                            <span className="sql-completion-text">{c.text}</span>
                            {c.detail ? <span className="sql-completion-detail">{c.detail}</span> : null}
                        </li>
                    ))}
                </ul>
            ) : null}
        </div>
    );
}
