import { useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import {
    CircleCheck,
    Clipboard,
    Download,
    FileCode,
    FileText,
    LayoutGrid,
    MoreHorizontal,
    Play,
    Plus,
    Save,
    Server,
    Square,
    Upload,
    Workflow,
    X,
    Zap,
    Home,
} from 'lucide-react';

export type Job = {
    id: string;
    name: string;
    dirty: boolean;
};

type Props = {
    jobs: Job[];
    activeJobId: string;
    isRunning: boolean;
    onSelectJob: (id: string) => void;
    onCloseJob: (id: string) => void;
    onNewJob: () => void;
    onRun: () => void;
    onStop: () => void;
    liveMode: boolean;
    onToggleLive: () => void;
    onSave: () => void;
    onValidate: () => void;
    onAutoLayout: () => void;
    onCopySql: () => void;
    onExportJson: () => void;
    /** Undefined in the web editor: deploying goes through a desktop-only command. */
    onDeploy?: () => void;
    onExportSqlFile: () => void;
    onGenerateDocs: () => void;
    onImportJson: () => void;
    /** Undefined in the web editor, where the engine-side translator is unreachable. */
    onImportJob?: () => void;
    /** Reopen the Home launcher. */
    onGoHome: () => void;
};

export default function EditorHeader({
    jobs,
    activeJobId,
    isRunning,
    onSelectJob,
    onCloseJob,
    onNewJob,
    onRun,
    onStop,
    liveMode,
    onToggleLive,
    onSave,
    onValidate,
    onAutoLayout,
    onCopySql,
    onExportJson,
    onDeploy,
    onExportSqlFile,
    onGenerateDocs,
    onImportJson,
    onImportJob,
    onGoHome,
}: Props) {
    const { t } = useTranslation();
    const [moreOpen, setMoreOpen] = useState(false);
    const moreRef = useRef<HTMLDivElement>(null);

    useEffect(() => {
        if (!moreOpen) return;
        const onClick = (e: MouseEvent) => {
            if (moreRef.current && !moreRef.current.contains(e.target as Node)) {
                setMoreOpen(false);
            }
        };
        const onKey = (e: KeyboardEvent) => {
            if (e.key === 'Escape') setMoreOpen(false);
        };
        document.addEventListener('mousedown', onClick);
        document.addEventListener('keydown', onKey);
        return () => {
            document.removeEventListener('mousedown', onClick);
            document.removeEventListener('keydown', onKey);
        };
    }, [moreOpen]);

    const fire = (fn: () => void) => () => {
        setMoreOpen(false);
        fn();
    };

    return (
        <div className="editor-header">
            <div className="job-tabs" role="tablist" aria-label="Open pipelines">
                {jobs.map(job => {
                    const isActive = job.id === activeJobId;
                    return (
                        <div
                            key={job.id}
                            className={'job-tab' + (isActive ? ' is-active' : '')}
                            role="tab"
                            aria-selected={isActive}
                        >
                            <button
                                type="button"
                                className="job-tab-button"
                                onClick={() => onSelectJob(job.id)}
                            >
                                <Workflow size={12} className="job-tab-icon" aria-hidden="true" />
                                <span className="job-tab-name">{job.name}</span>
                                {job.dirty ? (
                                    <span
                                        className="job-tab-dirty"
                                        aria-label={t('header.unsavedChanges')}
                                    />
                                ) : null}
                            </button>
                            <button
                                type="button"
                                className="job-tab-close"
                                onClick={() => onCloseJob(job.id)}
                                aria-label={t('header.closeTab', { name: job.name })}
                            >
                                <X size={12} />
                            </button>
                        </div>
                    );
                })}
                <button
                    type="button"
                    className="job-tab-new"
                    onClick={onNewJob}
                    title={t('header.newPipeline')}
                    aria-label={t('header.newPipeline')}
                >
                    <Plus size={14} />
                </button>
            </div>

            <div className="toolbar">
                <button
                    type="button"
                    className="toolbar-icon-button"
                    data-tour="home"
                    onClick={onGoHome}
                    title={t('launcher.goHome', 'Home')}
                    aria-label={t('launcher.goHome', 'Home')}
                >
                    <Home size={14} />
                </button>
                <div className="toolbar-sep" />
                {isRunning ? (
                    <button
                        type="button"
                        className="toolbar-button toolbar-stop"
                        onClick={onStop}
                        title={t('header.stopTooltip')}
                    >
                        <Square size={11} fill="currentColor" />
                        <span>{t('header.stop')}</span>
                    </button>
                ) : (
                    <button
                        type="button"
                        className="toolbar-button toolbar-run"
                        data-tour="run"
                        onClick={onRun}
                        title={t('header.runTooltip')}
                    >
                        <Play size={11} fill="currentColor" />
                        <span>{t('header.run')}</span>
                    </button>
                )}

                <button
                    type="button"
                    data-tour="live"
                    className={
                        'toolbar-icon-button' + (liveMode ? ' is-active' : '')
                    }
                    onClick={onToggleLive}
                    title={liveMode ? t('header.liveOnTooltip') : t('header.liveTooltip')}
                    aria-label={t('header.live')}
                    aria-pressed={liveMode}
                >
                    <Zap size={14} />
                </button>

                <div className="toolbar-sep" />

                <button
                    type="button"
                    className="toolbar-icon-button"
                    data-tour="save"
                    onClick={onSave}
                    title={t('header.saveTooltip')}
                    aria-label={t('header.save')}
                >
                    <Save size={14} />
                </button>

                <button
                    type="button"
                    className="toolbar-icon-button"
                    onClick={onValidate}
                    title={t('header.validateTooltip')}
                    aria-label={t('header.validate')}
                >
                    <CircleCheck size={14} />
                </button>

                <button
                    type="button"
                    className="toolbar-icon-button"
                    onClick={onAutoLayout}
                    title={t('header.autoLayout')}
                    aria-label={t('header.autoLayout')}
                >
                    <LayoutGrid size={14} />
                </button>

                <div className="toolbar-more" ref={moreRef}>
                    <button
                        type="button"
                        className={
                            'toolbar-icon-button' + (moreOpen ? ' is-active' : '')
                        }
                        onClick={() => setMoreOpen(o => !o)}
                        title={t('header.moreTooltip')}
                        aria-label={t('header.more')}
                        aria-expanded={moreOpen}
                    >
                        <MoreHorizontal size={14} />
                    </button>
                    {moreOpen ? (
                        <div className="toolbar-more-menu" role="menu">
                            <button
                                type="button"
                                role="menuitem"
                                className="toolbar-more-item"
                                onClick={fire(onCopySql)}
                            >
                                <Clipboard size={13} />
                                <div>
                                    <div>{t('header.copySql')}</div>
                                    <div className="toolbar-more-desc">
                                        {t('header.copySqlDesc')}
                                    </div>
                                </div>
                            </button>
                            <button
                                type="button"
                                role="menuitem"
                                className="toolbar-more-item"
                                onClick={fire(onExportSqlFile)}
                            >
                                <FileCode size={13} />
                                <div>
                                    <div>{t('header.exportSql')}</div>
                                    <div className="toolbar-more-desc">
                                        {t('header.exportSqlDesc')}
                                    </div>
                                </div>
                            </button>
                            <button
                                type="button"
                                role="menuitem"
                                className="toolbar-more-item"
                                onClick={fire(onGenerateDocs)}
                            >
                                <FileText size={13} />
                                <div>
                                    <div>{t('header.generateDocs')}</div>
                                    <div className="toolbar-more-desc">
                                        {t('header.generateDocsDesc')}
                                    </div>
                                </div>
                            </button>
                            {onDeploy ? (
                                <button
                                    type="button"
                                    role="menuitem"
                                    className="toolbar-more-item"
                                    onClick={fire(onDeploy)}
                                >
                                    <Server size={13} />
                                    <div>
                                        <div>{t('header.deploy', 'Deploy to a server…')}</div>
                                        <div className="toolbar-more-desc">
                                            {t(
                                                'header.deployDesc',
                                                'Send this pipeline to a server you own',
                                            )}
                                        </div>
                                    </div>
                                </button>
                            ) : null}
                            <button
                                type="button"
                                role="menuitem"
                                className="toolbar-more-item"
                                onClick={fire(onExportJson)}
                            >
                                <Download size={13} />
                                <div>
                                    <div>{t('header.exportJson')}</div>
                                    <div className="toolbar-more-desc">
                                        {t('header.exportJsonDesc')}
                                    </div>
                                </div>
                            </button>
                            <button
                                type="button"
                                role="menuitem"
                                className="toolbar-more-item"
                                onClick={fire(onImportJson)}
                            >
                                <Upload size={13} />
                                <div>
                                    <div>{t('header.importJson')}</div>
                                    <div className="toolbar-more-desc">
                                        {t('header.importJsonDesc')}
                                    </div>
                                </div>
                            </button>
                            {onImportJob ? (
                                <button
                                    type="button"
                                    role="menuitem"
                                    className="toolbar-more-item"
                                    onClick={fire(onImportJob)}
                                >
                                    <Upload size={13} />
                                    <div>
                                        <div>{t('header.importJob', 'Import Talend job...')}</div>
                                        <div className="toolbar-more-desc">
                                            {t(
                                                'header.importJobDesc',
                                                'Translate a Talend .item job into a pipeline',
                                            )}
                                        </div>
                                    </div>
                                </button>
                            ) : null}
                        </div>
                    ) : null}
                </div>
            </div>
        </div>
    );
}
