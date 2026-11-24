import { useEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { Eye, FileText, Pencil, Save, X } from 'lucide-react';
import Editor from './code-editor';
import Prism from 'prismjs';
import 'prismjs/components/prism-markdown';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import type { DocumentPayload, RepoItem } from '../../repo-types';

type Props = {
    item: RepoItem | null;
    onSave: (name: string, payload: DocumentPayload) => void;
    onCancel: () => void;
};

const TEMPLATE = `# Pipeline notes

Describe what this pipeline does, its inputs, expected outputs, and any
operational caveats here.

## Schedule

How often does this run?

## Inputs

- Source A: …
- Source B: …

## Outputs

- Sink: …

## Code

\`\`\`sql
SELECT order_id, status, amount
FROM orders
WHERE status = 'paid';
\`\`\`
`;

export default function DocumentEditorModal({ item, onSave, onCancel }: Props) {
    const initial = item?.payload as DocumentPayload | undefined;
    const [name, setName] = useState(item?.name ?? '');
    const [content, setContent] = useState(initial?.content ?? TEMPLATE);
    const [mode, setMode] = useState<'edit' | 'preview'>('edit');
    const nameRef = useRef<HTMLInputElement>(null);

    useEffect(() => {
        setTimeout(() => nameRef.current?.focus(), 30);
        const onKey = (e: KeyboardEvent) => {
            if (e.key === 'Escape') onCancel();
        };
        document.addEventListener('keydown', onKey);
        return () => document.removeEventListener('keydown', onKey);
    }, [onCancel]);

    const canSave = name.trim().length > 0;

    const handleSave = () => {
        if (!canSave) return;
        onSave(name.trim(), { content });
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
                        <FileText size={16} className="modal-title-icon" />
                        <div>
                            <div className="modal-title">
                                {item ? 'Edit document' : 'New document'}
                            </div>
                            <div className="modal-subtitle">Markdown notes</div>
                        </div>
                    </div>
                    <div className="modal-mapper-header-actions">
                        <div className="filter-builder-modes" style={{ marginRight: 8 }}>
                            <button
                                type="button"
                                className={
                                    'filter-mode' + (mode === 'edit' ? ' is-active' : '')
                                }
                                onClick={() => setMode('edit')}
                            >
                                <Pencil size={11} /> Edit
                            </button>
                            <button
                                type="button"
                                className={
                                    'filter-mode' + (mode === 'preview' ? ' is-active' : '')
                                }
                                onClick={() => setMode('preview')}
                            >
                                <Eye size={11} /> Preview
                            </button>
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
                </div>

                <div className="modal-body modal-doc-body">
                    <div className="modal-field">
                        <label className="modal-field-label">Document name</label>
                        <input
                            ref={nameRef}
                            type="text"
                            className="modal-input"
                            value={name}
                            placeholder="e.g. orders_etl_runbook"
                            onChange={e => setName(e.target.value)}
                            spellCheck={false}
                        />
                    </div>

                    {mode === 'edit' ? (
                        <div className="code-editor-host">
                            <Editor
                                value={content}
                                onValueChange={setContent}
                                highlight={code =>
                                    Prism.highlight(code, Prism.languages.markdown, 'markdown')
                                }
                                padding={16}
                                className="code-editor"
                                textareaClassName="code-editor-textarea"
                            />
                        </div>
                    ) : (
                        <div className="markdown-body">
                            <ReactMarkdown
                                remarkPlugins={[remarkGfm]}
                                components={{
                                    img: ({ src, alt, title }) => (
                                        <img
                                            src={src}
                                            alt={alt ?? ''}
                                            title={title}
                                            loading="lazy"
                                            referrerPolicy="no-referrer"
                                            onError={e => {
                                                (
                                                    e.currentTarget as HTMLImageElement
                                                ).style.display = 'none';
                                            }}
                                        />
                                    ),
                                    a: ({ href, children, title }) => (
                                        <a
                                            href={href}
                                            title={title}
                                            target="_blank"
                                            rel="noopener noreferrer"
                                        >
                                            {children}
                                        </a>
                                    ),
                                }}
                            >
                                {content}
                            </ReactMarkdown>
                        </div>
                    )}
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
