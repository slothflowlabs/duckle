import { useEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { Code2, Save, X } from 'lucide-react';
import Editor from './code-editor';
import Prism from 'prismjs';
import 'prismjs/components/prism-python';
import 'prismjs/components/prism-rust';
import 'prismjs/components/prism-sql';
import 'prismjs/components/prism-bash';
import type { RepoItem, RoutineLanguage, RoutinePayload } from '../../repo-types';

type Props = {
    item: RepoItem | null;
    onSave: (name: string, payload: RoutinePayload) => void;
    onCancel: () => void;
};

const LANG_OPTIONS: { value: RoutineLanguage; label: string }[] = [
    { value: 'python', label: 'Python' },
    { value: 'rust', label: 'Rust' },
    { value: 'javascript', label: 'JavaScript' },
    { value: 'sql', label: 'SQL' },
    { value: 'bash', label: 'Bash' },
];

const TEMPLATES: Record<RoutineLanguage, string> = {
    python: `# Reusable Python helper.\n# Call this from a Python UDF node with: from routines import <name>\n\ndef transform(row):\n    return row\n`,
    rust: `// Reusable Rust helper.\n// Compiled into a UDF crate.\n\npub fn transform(row: serde_json::Value) -> serde_json::Value {\n    row\n}\n`,
    javascript: `// Reusable JS helper.\n\nexport function transform(row) {\n    return row;\n}\n`,
    sql: `-- Reusable SQL snippet.\n-- Reference via {{ routines.<name> }} in SQL nodes.\n\nSELECT *\nFROM input_table\n`,
    bash: `#!/usr/bin/env bash\n# Reusable shell snippet.\n\necho "hello"\n`,
};

function prismLanguageFor(lang: RoutineLanguage): Prism.Grammar {
    switch (lang) {
        case 'python':
            return Prism.languages.python ?? Prism.languages.clike;
        case 'rust':
            return Prism.languages.rust ?? Prism.languages.clike;
        case 'sql':
            return Prism.languages.sql ?? Prism.languages.clike;
        case 'bash':
            return Prism.languages.bash ?? Prism.languages.clike;
        case 'javascript':
        default:
            return Prism.languages.javascript ?? Prism.languages.clike;
    }
}

export default function RoutineEditorModal({ item, onSave, onCancel }: Props) {
    const initial = item?.payload as RoutinePayload | undefined;
    const [name, setName] = useState(item?.name ?? '');
    const [language, setLanguage] = useState<RoutineLanguage>(initial?.language ?? 'python');
    const [code, setCode] = useState(initial?.code ?? TEMPLATES[initial?.language ?? 'python']);
    const [description, setDescription] = useState(initial?.description ?? '');
    const nameRef = useRef<HTMLInputElement>(null);

    useEffect(() => {
        setTimeout(() => nameRef.current?.focus(), 30);
        const onKey = (e: KeyboardEvent) => {
            if (e.key === 'Escape') onCancel();
        };
        document.addEventListener('keydown', onKey);
        return () => document.removeEventListener('keydown', onKey);
    }, [onCancel]);

    const handleLanguageChange = (next: RoutineLanguage) => {
        if (Object.values(TEMPLATES).includes(code) || code.trim() === '') {
            setCode(TEMPLATES[next]);
        }
        setLanguage(next);
    };

    const canSave = name.trim().length > 0;

    const handleSave = () => {
        if (!canSave) return;
        onSave(name.trim(), {
            language,
            code,
            description: description.trim() || undefined,
        });
    };

    return createPortal(
        <div
            className="modal-backdrop"
            onClick={e => {
                if (e.target === e.currentTarget) onCancel();
            }}
        >
            <div className="modal modal-editor modal-editor-tall">
                <div className="modal-header">
                    <div className="modal-title-row">
                        <Code2 size={16} className="modal-title-icon" />
                        <div>
                            <div className="modal-title">
                                {item ? 'Edit routine' : 'New routine'}
                            </div>
                            <div className="modal-subtitle">
                                Reusable helper code referenced from custom-code nodes
                            </div>
                        </div>
                    </div>
                    <button
                        type="button"
                        className="modal-close"
                        onClick={onCancel}
                        aria-label="Close"
                    >
                        <X size={16} />
                    </button>
                </div>

                <div className="modal-body modal-doc-body">
                    <div className="connection-field-grid">
                        <div className="modal-field">
                            <label className="modal-field-label">Routine name</label>
                            <input
                                ref={nameRef}
                                type="text"
                                className="modal-input"
                                value={name}
                                placeholder="e.g. normalize_phone"
                                onChange={e => setName(e.target.value)}
                                spellCheck={false}
                            />
                        </div>
                        <div className="modal-field">
                            <label className="modal-field-label">Language</label>
                            <select
                                className="modal-input modal-select"
                                value={language}
                                onChange={e =>
                                    handleLanguageChange(e.target.value as RoutineLanguage)
                                }
                            >
                                {LANG_OPTIONS.map(l => (
                                    <option key={l.value} value={l.value}>
                                        {l.label}
                                    </option>
                                ))}
                            </select>
                        </div>
                    </div>

                    <div className="modal-field">
                        <label className="modal-field-label">Description (optional)</label>
                        <input
                            type="text"
                            className="modal-input"
                            value={description}
                            placeholder="What does this routine do?"
                            onChange={e => setDescription(e.target.value)}
                            spellCheck={false}
                        />
                    </div>

                    <div className="code-editor-host">
                        <Editor
                            value={code}
                            onValueChange={setCode}
                            highlight={c => Prism.highlight(c, prismLanguageFor(language), language)}
                            padding={16}
                            className="code-editor"
                            textareaClassName="code-editor-textarea"
                        />
                    </div>
                </div>

                <div className="modal-footer">
                    <button type="button" className="btn btn-secondary" onClick={onCancel}>
                        Cancel
                    </button>
                    <button
                        type="button"
                        className="btn btn-primary"
                        onClick={handleSave}
                        disabled={!canSave}
                    >
                        <Save size={13} />
                        Save
                    </button>
                </div>
            </div>
        </div>,
        document.body,
    );
}
