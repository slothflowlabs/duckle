import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Boxes, FolderTree } from 'lucide-react';
import Palette from './Palette';
import ProjectTree from './ProjectTree';
import type { RepoItem } from '../repo-types';

type SideTab = 'project' | 'palette';

type Props = {
    repoItems: RepoItem[];
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
    onRenameRepoItem: (id: string, newName: string) => void;
    onDuplicateRepoItem: (id: string) => void;
    onDeleteRepoItem: (id: string) => void;
    onSchedulePipeline: (id: string) => void;
    onBuildPipeline: (id: string) => void;
};

export default function LeftSidebar({
    repoItems,
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
    onRenameRepoItem,
    onDuplicateRepoItem,
    onDeleteRepoItem,
    onSchedulePipeline,
    onBuildPipeline,
}: Props) {
    const { t } = useTranslation();
    const [tab, setTab] = useState<SideTab>('palette');

    return (
        <aside className="left-sidebar">
            <div className="left-sidebar-tabs" role="tablist" aria-label={t('sidebar.ariaLabel')}>
                <button
                    type="button"
                    role="tab"
                    aria-selected={tab === 'project'}
                    className="left-sidebar-tab"
                    onClick={() => setTab('project')}
                >
                    <FolderTree className="left-sidebar-tab-icon" size={13} aria-hidden="true" />
                    {t('sidebar.project')}
                </button>
                <button
                    type="button"
                    role="tab"
                    aria-selected={tab === 'palette'}
                    className="left-sidebar-tab"
                    onClick={() => setTab('palette')}
                >
                    <Boxes className="left-sidebar-tab-icon" size={13} aria-hidden="true" />
                    {t('sidebar.components')}
                </button>
            </div>
            <div className="left-sidebar-body">
                {tab === 'palette' ? (
                    <Palette />
                ) : (
                    <ProjectTree
                        items={repoItems}
                        activeJobId={activeJobId}
                        openJobIds={openJobIds}
                        onOpenPipeline={onOpenPipeline}
                        onOpenItem={onOpenItem}
                        onNewPipeline={onNewPipeline}
                        onNewFolder={onNewFolder}
                        onNewConnection={onNewConnection}
                        onNewContext={onNewContext}
                        onNewDocument={onNewDocument}
                        onNewRoutine={onNewRoutine}
                        onRename={onRenameRepoItem}
                        onDuplicate={onDuplicateRepoItem}
                        onDelete={onDeleteRepoItem}
                        onSchedulePipeline={onSchedulePipeline}
                        onBuildPipeline={onBuildPipeline}
                    />
                )}
            </div>
        </aside>
    );
}
