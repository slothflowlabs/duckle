import { useState } from 'react';
import type { Field } from './types';
import { connectionPlaceholder, useConnectionSupplied } from './useConnectionSupplied';

type Props<T> = {
    field: Field;
    value: T | undefined;
    onChange: (v: T | undefined) => void;
};

// A field holds a secret when explicitly flagged, or by the long-standing
// convention that password / token / key inputs use the bullet placeholder.
function isSecretField(field: Field): boolean {
    return field.secret === true || field.placeholder === '••••••••';
}

export function TextField({ field, value, onChange }: Props<string>) {
    const secret = isSecretField(field);
    const [reveal, setReveal] = useState(false);
    // Shown, never stored. See useConnectionSupplied.
    const supplied = useConnectionSupplied(field.key);
    const ph = connectionPlaceholder(supplied, field.placeholder, secret);
    if (!secret) {
        return (
            <input
                type="text"
                className="field-input"
                value={value ?? ''}
                placeholder={ph}
                onChange={e => onChange(e.target.value)}
                spellCheck={false}
            />
        );
    }
    return (
        <div className="field-secret">
            <input
                type={reveal ? 'text' : 'password'}
                className="field-input"
                value={value ?? ''}
                placeholder={ph}
                onChange={e => onChange(e.target.value)}
                spellCheck={false}
                autoComplete="off"
            />
            <button
                type="button"
                className="field-secret-toggle"
                onClick={() => setReveal(r => !r)}
                aria-label={reveal ? 'Hide' : 'Show'}
                title={reveal ? 'Hide' : 'Show'}
                tabIndex={-1}
            >
                {reveal ? 'Hide' : 'Show'}
            </button>
        </div>
    );
}

export function TextareaField({ field, value, onChange }: Props<string>) {
    return (
        <textarea
            className={'field-input field-textarea' + (field.monospace ? ' field-mono' : '')}
            value={value ?? ''}
            placeholder={field.placeholder}
            rows={field.rows ?? 3}
            onChange={e => onChange(e.target.value)}
            spellCheck={false}
        />
    );
}

// An emptied box means "not set", not zero. Sending 0 made the field
// unclearable - `value ?? ''` re-rendered it as "0" the moment it was
// emptied - and put a 0 in the pipeline that the user never typed. The
// engine reads these with `.filter(|n| *n > 0)` almost everywhere, which
// says the same thing: 0 is not a value, it is the absence of one.
export function NumberField({ field, value, onChange }: Props<number>) {
    const supplied = useConnectionSupplied(field.key);
    const ph = connectionPlaceholder(supplied, field.placeholder, false);
    return (
        <input
            type="number"
            className="field-input"
            value={value ?? ''}
            placeholder={ph}
            onChange={e => {
                if (e.target.value === '') return onChange(undefined);
                const n = Number(e.target.value);
                onChange(Number.isFinite(n) ? n : undefined);
            }}
        />
    );
}

export function IntegerField({ field, value, onChange }: Props<number>) {
    const supplied = useConnectionSupplied(field.key);
    const ph = connectionPlaceholder(supplied, field.placeholder, false);
    return (
        <input
            type="number"
            step={1}
            className="field-input"
            value={value ?? ''}
            placeholder={ph}
            onChange={e => {
                if (e.target.value === '') return onChange(undefined);
                const n = parseInt(e.target.value, 10);
                onChange(Number.isFinite(n) ? n : undefined);
            }}
        />
    );
}

export function BoolField({ field, value, onChange }: Props<boolean>) {
    return (
        <label className="field-toggle">
            <input
                type="checkbox"
                checked={value ?? false}
                onChange={e => onChange(e.target.checked)}
            />
            <span className="field-toggle-label">{field.placeholder ?? 'Enabled'}</span>
        </label>
    );
}

export function SelectField({ field, value, onChange }: Props<string>) {
    // A plain select cannot express "one of these, or something else", and some
    // of these values are not a closed set: a delimiter can be any character a
    // system chose to write, and DuckDB accepts over a thousand encoding names.
    // The options stay - they are what people pick most of the time - and the
    // field is typable for the file that does not fit any of them.
    if (field.allowCustom) {
        const listId = `duckle-opts-${field.key}`;
        return (
            <>
                <input
                    className="field-input"
                    list={listId}
                    value={value ?? ''}
                    placeholder={field.placeholder}
                    spellCheck={false}
                    onChange={e => onChange(e.target.value)}
                />
                <datalist id={listId}>
                    {field.options?.map(o => (
                        <option key={o.value} value={o.value} label={o.label} />
                    ))}
                </datalist>
            </>
        );
    }
    return (
        <select
            className="field-input field-select"
            value={value ?? ''}
            onChange={e => onChange(e.target.value)}
        >
            {field.options?.map(o => (
                <option key={o.value} value={o.value}>
                    {o.label}
                </option>
            ))}
        </select>
    );
}
