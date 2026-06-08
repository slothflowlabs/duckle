import { useEffect, useMemo, useRef, useState } from 'react';
import {
    AlarmClock,
    Box,
    ChevronDown,
    ChevronRight,
    Code2,
    Copy,
    FileCog,
    FileText,
    Folder,
    FolderOpen,
    FolderPlus,
    Package,
    Pencil,
    Plug,
    Plus,
    Trash2,
    Variable,
    Workflow,
    ArrowUpRight,
} from 'lucide-react';
import type { RepoItem, RepoItemType } from '../repo-types';
import { useContextMenu, type MenuItem } from './ContextMenu';

const ICON_SIZE = 14;

type Props = {
    items: RepoItem[];
    activeJobId: string;
    openJobIds: Set<string>;
    onOpenPipeline: (id: string) => void;
    onOpenItem: (item: RepoItem) => void;
    onNewPipeline: (parentId: string) => void;
    onNewFolder: (parentId: string) => void;
    onNewConnection: (parentId: string) => void;
    onNewContext: (parentId: string) => void;
    onNewDocument: (parentId: string) => void;
    onNewRoutine: (parentId: string) => void;
    onRename: (id: string, newName: string) => void;
    onDuplicate: (id: string) => void;
    onDelete: (id: string) => void;
    onSchedulePipeline: (id: string) => void;
    onBuildPipeline: (id: string) => void;
};

const TYPE_LABEL: Record<RepoItemType, string> = {
    project: 'Project',
    folder: 'Folder',
    pipeline: 'Pipeline',
    connection: 'Connection',
    context: 'Context',
    routine: 'Routine',
    doc: 'Document',
};

function TypeIcon({ type, isOpen }: { type: RepoItemType; isOpen: boolean }) {
    const size = ICON_SIZE;
    switch (type) {
        case 'project':
            return <Box size={size} />;
        case 'folder':
            return isOpen ? <FolderOpen size={size} /> : <Folder size={size} />;
        case 'pipeline':
            return <Workflow size={size} />;
        case 'connection':
            return <Plug size={size} />;
        case 'context':
            return <Variable size={size} />;
        case 'routine':
            return <Code2 size={size} />;
        case 'doc':
            return <FileText size={size} />;
    }
}

export default function ProjectTree(props: Props) {
    const {
        items,
        activeJobId,
        openJobIds,
        onOpenPipeline,
        onOpenItem,
        onNewPipeline,
        onNewFolder,
        onNewConnection,
        onNewContext,
        onNewDocument,
        onNewRoutine,
        onRename,
        onDuplicate,
        onDelete,
        onSchedulePipeline,
        onBuildPipeline,
    } = props;

    // Walk up to find which root folder this item lives under.
    const rootFolderOf = (itemId: string): string | null => {
        let current = items.find(i => i.id === itemId);
        while (current) {
            if (current.parentId === 'root') return current.id;
            if (!current.parentId) return null;
            current = items.find(i => i.id === current!.parentId);
        }
        return null;
    };

    const [expanded, setExpanded] = useState<Set<string>>(
        () => new Set(items.filter(i => i.type === 'project' || i.type === 'folder').map(i => i.id)),
    );
    const [renamingId, setRenamingId] = useState<string | null>(null);
    const [draftName, setDraftName] = useState('');
    const menu = useContextMenu();

    const childrenOf = useMemo(() => {
        const map = new Map<string, RepoItem[]>();
        for (const item of items) {
            const key = item.parentId ?? '__root__';
            const list = map.get(key) ?? [];
            list.push(item);
            map.set(key, list);
        }
        for (const [, list] of map) {
            list.sort((a, b) => {
                const folderFirst = (b.type === 'folder' ? 1 : 0) - (a.type === 'folder' ? 1 : 0);
                if (folderFirst !== 0) return folderFirst;
                return a.name.localeCompare(b.name);
            });
        }
        return map;
    }, [items]);

    const startRename = (id: string) => {
        const item = items.find(i => i.id === id);
        if (!item || item.type === 'project') return;
        setRenamingId(id);
        setDraftName(item.name);
    };

    const commitRename = () => {
        if (!renamingId) return;
        const trimmed = draftName.trim();
        if (trimmed && trimmed !== items.find(i => i.id === renamingId)?.name) {
            onRename(renamingId, trimmed);
        }
        setRenamingId(null);
    };

    const cancelRename = () => setRenamingId(null);

    const toggle = (id: string) => {
        setExpanded(s => {
            const next = new Set(s);
            if (next.has(id)) next.delete(id);
            else next.add(id);
            return next;
        });
    };

    const buildFolderMenu = (item: RepoItem): MenuItem[] => {
        const root = item.type === 'project' ? null : rootFolderOf(item.id) ?? item.id;
        const isPipelinesScope = item.id === 'pipelines' || root === 'pipelines';
        const isConnectionsScope = item.id === 'connections' || root === 'connections';
        const isContextsScope = item.id === 'contexts' || root === 'contexts';
        const isRoutinesScope = item.id === 'routines' || root === 'routines';
        const isDocsScope = item.id === 'docs' || root === 'docs';

        const newItems: MenuItem[] = [];
        if (item.type === 'project' || isPipelinesScope) {
            newItems.push({
                kind: 'item',
                key: 'new-pipeline',
                label: 'New pipeline…',
                icon: <FileCog size={ICON_SIZE} />,
                onClick: () => onNewPipeline(item.id),
            });
        }
        if (item.type === 'project' || isConnectionsScope) {
            newItems.push({
                kind: 'item',
                key: 'new-connection',
                label: 'New connection…',
                icon: <FileCog size={ICON_SIZE} />,
                onClick: () => onNewConnection(item.id),
            });
        }
        if (item.type === 'project' || isContextsScope) {
            newItems.push({
                kind: 'item',
                key: 'new-context',
                label: 'New context…',
                icon: <FileCog size={ICON_SIZE} />,
                onClick: () => onNewContext(item.id),
            });
        }
        if (item.type === 'project' || isRoutinesScope) {
            newItems.push({
                kind: 'item',
                key: 'new-routine',
                label: 'New routine…',
                icon: <FileCog size={ICON_SIZE} />,
                onClick: () => onNewRoutine(item.id),
            });
        }
        if (item.type === 'project' || isDocsScope) {
            newItems.push({
                kind: 'item',
                key: 'new-document',
                label: 'New document…',
                icon: <FileCog size={ICON_SIZE} />,
                onClick: () => onNewDocument(item.id),
            });
        }
        newItems.push({
            kind: 'item',
            key: 'new-folder',
            label: 'New folder',
            icon: <FolderPlus size={ICON_SIZE} />,
            onClick: () => onNewFolder(item.id),
        });

        return [
            { kind: 'header', key: 'h', label: TYPE_LABEL[item.type] + ': ' + item.name },
            ...newItems,
            { kind: 'separator', key: 's1' },
            {
                kind: 'item',
                key: 'rename',
                label: 'Rename',
                icon: <Pencil size={ICON_SIZE} />,
                shortcut: 'F2',
                onClick: () => startRename(item.id),
                disabled: item.type === 'project',
            },
            {
                kind: 'item',
                key: 'delete',
                label: 'Delete',
                icon: <Trash2 size={ICON_SIZE} />,
                shortcut: 'Del',
                onClick: () => onDelete(item.id),
                danger: true,
                disabled: item.type === 'project',
            },
        ];
    };

    const buildItemMenu = (item: RepoItem): MenuItem[] => {
        const items: MenuItem[] = [
            { kind: 'header', key: 'h', label: TYPE_LABEL[item.type] + ': ' + item.name },
            {
                kind: 'item',
                key: 'open',
                label: 'Open',
                icon: <ArrowUpRight size={ICON_SIZE} />,
                shortcut: 'Enter',
                onClick: () => onOpenPipeline(item.id),
                disabled: item.type !== 'pipeline',
            },
        ];
        if (item.type === 'pipeline') {
            items.push({
                kind: 'item',
                key: 'schedule',
                label: 'Schedule…',
                icon: <AlarmClock size={ICON_SIZE} />,
                onClick: () => onSchedulePipeline(item.id),
            });
            items.push({
                kind: 'item',
                key: 'build',
                label: 'Build pipeline…',
                icon: <Package size={ICON_SIZE} />,
                onClick: () => onBuildPipeline(item.id),
            });
        }
        items.push({
            kind: 'item',
            key: 'duplicate',
            label: 'Duplicate',
            icon: <Copy size={ICON_SIZE} />,
            shortcut: 'Ctrl+D',
            onClick: () => onDuplicate(item.id),
        });
        items.push({ kind: 'separator', key: 's1' });
        return items;
    };

    const finishItemMenu = (base: MenuItem[], item: RepoItem): MenuItem[] => [
        ...base,
        {
            kind: 'item',
            key: 'rename',
            label: 'Rename',
            icon: <Pencil size={ICON_SIZE} />,
            shortcut: 'F2',
            onClick: () => startRename(item.id),
        },
        {
            kind: 'item',
            key: 'delete',
            label: 'Delete',
            icon: <Trash2 size={ICON_SIZE} />,
            shortcut: 'Del',
            onClick: () => onDelete(item.id),
            danger: true,
        },
    ];

    const onItemContextMenu = (e: React.MouseEvent, item: RepoItem) => {
        const itemsArr =
            item.type === 'folder' || item.type === 'project'
                ? buildFolderMenu(item)
                : finishItemMenu(buildItemMenu(item), item);
        menu.open(e, itemsArr);
    };

    const renderNode = (item: RepoItem, depth: number): React.ReactNode => {
        const isContainer = item.type === 'project' || item.type === 'folder';
        const isExpanded = isContainer ? expanded.has(item.id) : false;
        const children = childrenOf.get(item.id) ?? [];
        const isActive = item.type === 'pipeline' && item.id === activeJobId;
        const isOpen = item.type === 'pipeline' && openJobIds.has(item.id);
        const isRenaming = renamingId === item.id;

        const onClick = () => {
            if (isRenaming) return;
            if (isContainer) toggle(item.id);
            else if (item.type === 'pipeline') onOpenPipeline(item.id);
            else onOpenItem(item);
        };
        const onDoubleClick = () => {
            if (item.type === 'pipeline') onOpenPipeline(item.id);
            else if (!isContainer) onOpenItem(item);
        };

        return (
            <div key={item.id} className="repo-node-wrap">
                <div
                    className={
                        'repo-node' +
                        (isActive ? ' is-active' : '') +
                        (isOpen ? ' is-open' : '') +
                        ' is-' + item.type
                    }
                    style={{ paddingLeft: 8 + depth * 14 }}
                    draggable={item.type === 'context'}
                    onDragStart={
                        item.type === 'context'
                            ? e => {
                                  e.dataTransfer.setData('application/duckle-context', item.id);
                                  e.dataTransfer.effectAllowed = 'copy';
                              }
                            : undefined
                    }
                    onClick={onClick}
                    onDoubleClick={onDoubleClick}
                    onContextMenu={e => onItemContextMenu(e, item)}
                    title={
                        item.type === 'context'
                            ? `${item.name} - drag onto the canvas to make it the active context`
                            : item.name
                    }
                >
                    <span className="repo-chevron" aria-hidden="true">
                        {isContainer ? (
                            isExpanded ? (
                                <ChevronDown size={12} />
                            ) : (
                                <ChevronRight size={12} />
                            )
                        ) : null}
                    </span>
                    <span className={'repo-icon repo-icon-' + item.type} aria-hidden="true">
                        <TypeIcon type={item.type} isOpen={isExpanded} />
                    </span>
                    {isRenaming ? (
                        <RenameInput
                            value={draftName}
                            onChange={setDraftName}
                            onCommit={commitRename}
                            onCancel={cancelRename}
                        />
                    ) : (
                        <span className="repo-label">{item.name}</span>
                    )}
                    {item.type === 'pipeline' && isOpen && !isRenaming ? (
                        <span className="repo-open-dot" aria-label="open in editor" />
                    ) : null}
                    {item.type === 'folder' && children.length > 0 && !isRenaming ? (
                        <span className="repo-count">{children.length}</span>
                    ) : null}
                </div>
                {isContainer && isExpanded
                    ? children.map(child => renderNode(child, depth + 1))
                    : null}
            </div>
        );
    };

    const roots = items.filter(i => !i.parentId);

    return (
        <div className="repo-tree">
            <div className="repo-tree-actions">
                <button
                    type="button"
                    className="repo-action-button"
                    onClick={() => onNewPipeline('pipelines')}
                    title="New pipeline"
                >
                    <Plus size={13} /> Pipeline
                </button>
                <button
                    type="button"
                    className="repo-action-button"
                    onClick={() => onNewFolder('root')}
                    title="New folder"
                >
                    <FolderPlus size={13} /> Folder
                </button>
            </div>
            <div className="repo-tree-body" onContextMenu={e => e.preventDefault()}>
                {roots.map(r => renderNode(r, 0))}
            </div>
            {menu.element}
        </div>
    );
}

type RenameInputProps = {
    value: string;
    onChange: (v: string) => void;
    onCommit: () => void;
    onCancel: () => void;
};

function RenameInput({ value, onChange, onCommit, onCancel }: RenameInputProps) {
    const ref = useRef<HTMLInputElement>(null);
    useEffect(() => {
        ref.current?.focus();
        ref.current?.select();
    }, []);
    return (
        <input
            ref={ref}
            type="text"
            className="repo-rename-input"
            value={value}
            onChange={e => onChange(e.target.value)}
            onKeyDown={e => {
                if (e.key === 'Enter') {
                    e.preventDefault();
                    onCommit();
                } else if (e.key === 'Escape') {
                    e.preventDefault();
                    onCancel();
                }
                e.stopPropagation();
            }}
            onBlur={onCommit}
            onClick={e => e.stopPropagation()}
            spellCheck={false}
        />
    );
}
