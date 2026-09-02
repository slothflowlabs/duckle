import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { invoke } from '@tauri-apps/api/core';
import {
    addEdge,
    applyEdgeChanges,
    applyNodeChanges,
    type Connection,
    type Edge,
    type EdgeChange,
    type Node,
    type NodeChange,
    type OnSelectionChangeParams,
} from '@xyflow/react';
import type { ConnectionType } from './canvas/connection-types';
import { BarChart3, Braces, FolderOpen, GitBranch, LayoutDashboard, Loader2, Moon, RotateCw, ShieldCheck, Sparkles, Sun, Waypoints } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import LanguageSelector from './i18n/LanguageSelector';
import { UpdateBanner } from './UpdateBanner';
import { EngineUpgradeBanner } from './EngineUpgradeBanner';
import { ReviewPrompt } from './ReviewPrompt';
import EditorTabs from './workflow-ui/EditorTabs';
import EditorHeader, { type Job } from './workflow-ui/EditorHeader';
import EngineSelector, { normalizeEngineId, type EngineId } from './workflow-ui/EngineSelector';
import { useTheme } from './theme';
import { loadPersisted, savePersisted } from './persistence';
import { resolveOutputSchema } from './schema-resolve';
import {
    cancelPipeline,
    compilePipelineSql,
    runPipeline,
    runPipelinePartial,
    scheduleSetWorkspace,
    settingsLoadContextVars,
    type PipelineEvent,
    type RunResult,
} from './tauri-bridge';
import ScheduleEditorModal from './workflow-ui/ScheduleEditorModal';
import PlansModal from './workflow-ui/PlansModal';
import DeployModal from './workflow-ui/DeployModal';
import BackfillModal from './workflow-ui/BackfillModal';
import RunParametersModal from './workflow-ui/RunParametersModal';
import BuildPipelineModal from './workflow-ui/BuildPipelineModal';
import { McpModal } from './workflow-ui/McpModal';
import { SettingsModal } from './workflow-ui/SettingsModal';
import { Settings as SettingsIcon } from 'lucide-react';
import { ClaudeIcon } from './workflow-ui/ClaudeIcon';
import { DuckleLogo } from './workflow-ui/DuckleLogo';
import EngineSetupModal from './workflow-ui/EngineSetupModal';
import SetupWizard from './workflow-ui/SetupWizard';
import ChatPanel from './workflow-ui/ChatPanel';
import GitPanel from './workflow-ui/GitPanel';
import WindowControls from './workflow-ui/WindowControls';
import WindowResizeHandles from './workflow-ui/WindowResizeHandles';
import { engineStatus, seedSampleWorkspace, importJobFile, externalComponents } from './tauri-bridge';
import ImportReportModal, { type ImportReport } from './workflow-ui/ImportReportModal';
import HomeLauncher from './workflow-ui/HomeLauncher';
import { copyText, saveTextFile } from './tauri-io';
import { writeClipboard, readClipboard, instantiateClipboard } from './clipboard';
import { RunStatusContext } from './canvas/run-status-context';
import { layoutByDependency } from './canvas/layout';
import { validatePipeline } from './validation';
import { resolveForRun, discoverParams, builtinVars, buildContextVars } from './run-resolve';
import WorkspacePickerModal from './workflow-ui/WorkspacePickerModal';
import { AccountChip, ProfileSetupModal } from './workflow-ui/AccountMenu';
import {
    type Account,
    loadAccounts,
    saveAccounts,
    loadActiveAccountId,
    saveActiveAccountId,
    newAccountId,
} from './accounts';
import {
    deleteItemPayload,
    deletePipelineFile,
    getWorkspacePath,
    hasBackend,
    isInTauri,
    loadWorkspace,
    saveItemPayload,
    saveMetadata,
    savePipelineFile,
    saveRepository,
    setWorkspacePath,
    WorkspaceLoadError,
} from './workspace';
import { openExternal } from './tauri-io';
import { GuidedTour, TOUR_FINISHED_EVENT, tourAlreadySeen } from './GuidedTour';
import { isWebBackend } from './web-fs';
import LeftSidebar from './workflow-ui/LeftSidebar';
import PropertiesPanel from './workflow-ui/PropertiesPanel';
import BottomPanel from './workflow-ui/BottomPanel';
import StatusBar from './workflow-ui/StatusBar';
import NewPipelineModal, { type PipelineTemplate } from './workflow-ui/NewPipelineModal';
import ConfirmModal from './workflow-ui/ConfirmModal';
import EdgeEditorModal from './canvas/EdgeEditorModal';
import VisualMapperModal, {
    type MapperState,
    type MappingRow,
    type LookupConfig,
} from './canvas/VisualMapperModal';
import ConnectionEditorModal from './workflow-ui/editors/ConnectionEditorModal';
import ContextEditorModal from './workflow-ui/editors/ContextEditorModal';
import DocumentEditorModal from './workflow-ui/editors/DocumentEditorModal';
import RoutineEditorModal from './workflow-ui/editors/RoutineEditorModal';
import type { Column } from './pipeline-types';
import { setExternalComponents, setExternalManifest } from './workflow-ui/palette-data';
import type { ComponentDef, NodeKind as PaletteKind } from './workflow-ui/palette-data';
import type {
    ConnectionPayload,
    ContextPayload,
    DocumentPayload,
    RoutinePayload,
} from './repo-types';
import { getDefaults, getManifest } from './workflow-ui/fields/component-manifests';
import type { DuckleNodeData } from './pipeline-types';
import type { DropPosition, NodeAction, PaneAction } from './canvas/Canvas';
import { useUndoRedo, type CanvasSnapshot } from './useUndoRedo';
import type { RepoItem } from './repo-types';
import { DiveModal } from './dives/DiveModal';
import type { Dive } from './dives/dive-types';
import { DashboardModal } from './dives/DashboardModal';
import type { Dashboard } from './dives/dashboard-types';
import { DivesGallery } from './dives/DivesGallery';
import { LineageModal } from './workflow-ui/LineageModal';
import { TrustModal } from './workflow-ui/TrustModal';
import CatalogPanel from './workflow-ui/CatalogPanel';

type RuntimeState = 'connecting' | 'ready' | 'offline';

type PipelineState = {
    nodes: Node<DuckleNodeData>[];
    edges: Edge[];
};

const SAMPLE_NODES: Node<DuckleNodeData>[] = [
    {
        id: 's1',
        type: 'source',
        position: { x: 60, y: 140 },
        data: {
            label: 'CSV',
            componentId: 'src.csv',
            // No path yet - set one and Autodetect. The subtitle and
            // schema fill in from the real file, they aren't faked.
            properties: { path: '${workspace}/data/orders.csv', hasHeader: true },
        },
    },
    {
        id: 't1',
        type: 'transform',
        position: { x: 340, y: 140 },
        data: {
            label: 'Filter',
            componentId: 'xf.filter',
            // Valid DuckDB SQL - single quotes for the string literal.
            properties: { predicate: "status = 'paid'" },
        },
    },
    {
        id: 'k1',
        type: 'sink',
        position: { x: 620, y: 140 },
        data: {
            label: 'Parquet',
            componentId: 'snk.parquet',
            properties: { path: '${workspace}/output/paid_orders.parquet' },
        },
    },
];

const SAMPLE_EDGES: Edge[] = [
    {
        id: 'e1',
        source: 's1',
        sourceHandle: 'main',
        target: 't1',
        targetHandle: 'main',
        type: 'duckle',
        data: { connectionType: 'main' },
    },
    {
        id: 'e2',
        source: 't1',
        sourceHandle: 'main',
        target: 'k1',
        targetHandle: 'main',
        type: 'duckle',
        data: { connectionType: 'main' },
    },
];

const SAMPLE_JOIN_NODES: Node<DuckleNodeData>[] = [
    {
        id: 's_orders',
        type: 'source',
        position: { x: 40, y: 80 },
        data: {
            label: 'Orders',
            componentId: 'src.csv',
            properties: { path: '${workspace}/data/orders.csv', hasHeader: true },
        },
    },
    {
        id: 's_regions',
        type: 'source',
        position: { x: 40, y: 220 },
        data: {
            label: 'Regions',
            componentId: 'src.csv',
            properties: { path: '${workspace}/data/regions.csv', hasHeader: true },
        },
    },
    {
        id: 'j1',
        type: 'transform',
        position: { x: 320, y: 150 },
        data: {
            label: 'Inner Join',
            componentId: 'xf.join.inner',
            properties: { leftKey: 'region', rightKey: 'region' },
        },
    },
    {
        id: 'g1',
        type: 'transform',
        position: { x: 560, y: 150 },
        data: {
            label: 'Group By',
            componentId: 'xf.groupby',
            properties: {
                groupKeys: ['region', 'manager'],
                aggregations: [{ column: 'amount', function: 'sum', alias: 'total_amount' }],
            },
        },
    },
    {
        id: 'k1',
        type: 'sink',
        position: { x: 800, y: 150 },
        data: {
            label: 'CSV',
            componentId: 'snk.csv',
            properties: {
                path: '${workspace}/output/region_summary.csv',
                hasHeader: true,
            },
        },
    },
];

const SAMPLE_JOIN_EDGES: Edge[] = [
    {
        id: 'e1',
        source: 's_orders',
        sourceHandle: 'main',
        target: 'j1',
        targetHandle: 'main',
        type: 'duckle',
        data: { connectionType: 'main' },
    },
    {
        id: 'e2',
        source: 's_regions',
        sourceHandle: 'main',
        target: 'j1',
        targetHandle: 'lookup',
        type: 'duckle',
        data: { connectionType: 'lookup' },
    },
    {
        id: 'e3',
        source: 'j1',
        sourceHandle: 'main',
        target: 'g1',
        targetHandle: 'main',
        type: 'duckle',
        data: { connectionType: 'main' },
    },
    {
        id: 'e4',
        source: 'g1',
        sourceHandle: 'main',
        target: 'k1',
        targetHandle: 'main',
        type: 'duckle',
        data: { connectionType: 'main' },
    },
];

const INITIAL_JOBS: Job[] = [{ id: 'j1', name: 'orders_etl', dirty: false }];

const INITIAL_PIPELINE_DATA: Record<string, PipelineState> = {
    j1: { nodes: SAMPLE_NODES, edges: SAMPLE_EDGES },
};

const INITIAL_REPO: RepoItem[] = [
    { id: 'root', name: 'Duckle Project', type: 'project' },
    { id: 'pipelines', name: 'Pipelines', type: 'folder', parentId: 'root' },
    { id: 'connections', name: 'Connections', type: 'folder', parentId: 'root' },
    { id: 'contexts', name: 'Contexts', type: 'folder', parentId: 'root' },
    { id: 'routines', name: 'Routines', type: 'folder', parentId: 'root' },
    { id: 'docs', name: 'Documentation', type: 'folder', parentId: 'root' },
    { id: 'dives', name: 'Dives', type: 'folder', parentId: 'root' },
    { id: 'dashboards', name: 'Dashboards', type: 'folder', parentId: 'root' },
    { id: 'j1', name: 'orders_etl', type: 'pipeline', parentId: 'pipelines' },
];

function paletteKindToFlowType(kind: PaletteKind): string {
    switch (kind) {
        case 'source':
            return 'source';
        case 'sink':
            return 'sink';
        case 'transform':
        case 'control':
        case 'quality':
        case 'custom':
            return 'transform';
    }
}

function freshId(prefix: string): string {
    return prefix + '_' + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2, 7);
}

function seedTemplate(template: PipelineTemplate): PipelineState {
    if (template === 'sample-csv-to-parquet') {
        return { nodes: SAMPLE_NODES, edges: SAMPLE_EDGES };
    }
    if (template === 'sample-join-groupby') {
        return { nodes: SAMPLE_JOIN_NODES, edges: SAMPLE_JOIN_EDGES };
    }
    return { nodes: [], edges: [] };
}

const EMPTY_PIPELINE: PipelineState = { nodes: [], edges: [] };

export default function App() {
    const { t } = useTranslation();
    const { theme, toggle: toggleTheme } = useTheme();
    const [runtime, setRuntime] = useState<RuntimeState>('connecting');
    const [engine, setEngine] = useState<EngineId>(() =>
        normalizeEngineId(loadPersisted<EngineId>('engine', 'duckdb')),
    );
    const [pipelineData, setPipelineData] = useState<Record<string, PipelineState>>(() =>
        loadPersisted('pipelines', INITIAL_PIPELINE_DATA),
    );
    const [selectedId, setSelectedId] = useState<string | null>(null);
    const [jobs, setJobs] = useState<Job[]>(() => loadPersisted('jobs', INITIAL_JOBS));
    const [activeJobId, setActiveJobId] = useState<string>(() =>
        loadPersisted('active-job', 'j1'),
    );
    const [isRunning, setIsRunning] = useState<boolean>(false);
    // Live mode: when on, editing a node's properties debounces a partial run
    // up to that node so the canvas previews refresh as you work. Refs let the
    // (stable) edit handler read the latest mode + in-flight flag and reach the
    // freshest run closure without being re-created on every keystroke.
    const [liveMode, setLiveMode] = useState<boolean>(false);
    const liveModeRef = useRef(liveMode);
    const isRunningRef = useRef(false);
    const liveTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
    const triggerLivePreviewRef = useRef<((nodeId: string) => void) | null>(null);
    useEffect(() => {
        liveModeRef.current = liveMode;
    }, [liveMode]);
    useEffect(() => {
        isRunningRef.current = isRunning;
    }, [isRunning]);
    useEffect(
        () => () => {
            if (liveTimerRef.current) clearTimeout(liveTimerRef.current);
        },
        [],
    );
    const [renameRequest, setRenameRequest] = useState<number>(0);
    const [repo, setRepo] = useState<RepoItem[]>(() => loadPersisted('repo', INITIAL_REPO));
    const [activeContextId, setActiveContextId] = useState<string | null>(() =>
        loadPersisted<string | null>('active-context', null),
    );

    // First-run boot gate: in Tauri we must confirm an execution engine
    // is installed before anything else. 'checking' until engine_status
    // returns; 'engine-setup' if DuckDB is missing; 'ready' once present
    // (or in browser, where engines aren't downloaded).
    const [engineGate, setEngineGate] = useState<'checking' | 'engine-setup' | 'ready'>(
        () => (isInTauri() ? 'checking' : 'ready'),
    );
    const [showChatPanel, setShowChatPanel] = useState(false);
    const [showGitPanel, setShowGitPanel] = useState(false);

    useEffect(() => {
        if (!isInTauri()) return;
        let cancelled = false;
        void engineStatus().then(list => {
            if (cancelled) return;
            const duck = list.find(e => e.id === 'duckdb');
            // A missing engine blocks (pipelines can't run). An OUTDATED engine
            // still runs, so don't force the install modal - let the app proceed
            // and let EngineUpgradeBanner offer a non-blocking upgrade instead.
            setEngineGate(duck?.installed || duck?.outdated ? 'ready' : 'engine-setup');
        });
        return () => {
            cancelled = true;
        };
    }, []);

    const [workspacePathState, setWorkspacePathState] = useState<string | null>(() => {
        // Multi-account: on cold start the active account's bound workspace is
        // authoritative (so reopening lands in the right account's context),
        // falling back to the last global path for pre-account installs.
        const accs = loadAccounts();
        const active = accs.find(a => a.id === loadActiveAccountId()) ?? accs[0];
        return active ? active.workspacePath ?? null : getWorkspacePath();
    });
    // In Tauri AND the web edition: stay un-ready until the workspace has
    // hydrated, so auto-save can't overwrite the server/disk files with empty
    // in-memory defaults. Pure browser (no backend) is ready immediately.
    const [workspaceReady, setWorkspaceReady] = useState<boolean>(!isInTauri() && !isWebBackend());
    // A structural workspace file (duckle.json / repository.json) failed to
    // parse - we stay un-ready so auto-save can't overwrite the good files,
    // and show a banner naming the file. `corruptFiles` holds per-item files
    // (a context/connection/pipeline) that were skipped so the rest could load.
    const [workspaceLoadError, setWorkspaceLoadError] = useState<string | null>(null);
    // Bumped by "Reload from disk" to re-trigger the load effect for the same path (#92).
    const [reloadNonce, setReloadNonce] = useState(0);
    // True while a brand-new workspace is being seeded with sample pipelines +
    // generated data (Tauri only), so we can show a blocking init overlay.
    const [seeding, setSeeding] = useState(false);
    const [corruptFiles, setCorruptFiles] = useState<string[]>([]);
    // ---- local multi-account profiles: username + optional avatar, each bound
    // to its own workspace (= data / pipeline context). Stored only on this
    // device, never transmitted; no password. ----
    const [accounts, setAccounts] = useState<Account[]>(loadAccounts);
    const [activeAccountId, setActiveAccountId] = useState<string | null>(loadActiveAccountId);
    useEffect(() => {
        saveAccounts(accounts);
    }, [accounts]);
    useEffect(() => {
        saveActiveAccountId(activeAccountId);
    }, [activeAccountId]);
    const activeAccount = accounts.find(a => a.id === activeAccountId) ?? accounts[0] ?? null;
    // Engine setup comes first; then (first run) account setup; then the
    // workspace picker.
    const showEngineSetup = isInTauri() && engineGate === 'engine-setup';
    const showProfileSetup = isInTauri() && engineGate === 'ready' && accounts.length === 0;
    const showWorkspacePicker =
        isInTauri() && engineGate === 'ready' && accounts.length > 0 && !workspacePathState;
    // The one question of the first run, asked after there is a workspace to answer it
    // about and never again once answered. Someone trying Duckle out should meet a canvas,
    // so "local" is a click that changes nothing.
    const [setupChoice, setSetupChoice] = useState<string | null>(() => {
        try {
            return localStorage.getItem('duckle.setupChoice');
        } catch {
            return 'local';
        }
    });
    const showSetupWizard =
        isInTauri() &&
        engineGate === 'ready' &&
        accounts.length > 0 &&
        !!workspacePathState &&
        !setupChoice;

    // Settings can ask the question again. The stored answer is cleared here rather than in
    // the modal, because the modal only writes localStorage and this component's state is
    // what actually decides whether the wizard is on screen; clearing one without the other
    // means nothing happens until the next restart.
    useEffect(() => {
        const reset = () => {
            try {
                localStorage.removeItem('duckle.setupChoice');
            } catch {
                /* storage turned off: the state change below still re-opens it this session */
            }
            setSetupChoice(null);
        };
        window.addEventListener('duckle:reset-setup', reset);
        return () => window.removeEventListener('duckle:reset-setup', reset);
    }, []);

    // Home launcher. It overlays the workspace rather than replacing it: the
    // first-run tour waits for [data-tour="canvas"] to appear and gives up for
    // good after 24s, and unmounting would also reset the editor tab and the
    // sidebar tab on every trip back to Home.
    const [showHomeOnStartup, setShowHomeOnStartup] = useState<boolean>(() =>
        loadPersisted<boolean>('showHomeOnStartup', true),
    );
    const [showHome, setShowHome] = useState(false);
    // Open it once, after the startup gates have cleared, so Home is never
    // stacked behind the engine, profile or workspace dialogs.
    const homeShownRef = useRef(false);
    useEffect(() => {
        if (homeShownRef.current) return;
        if (showEngineSetup || showProfileSetup || showWorkspacePicker) return;
        if (isInTauri() && !workspacePathState) return;
        // On a first run the tour goes first and Home waits its turn. Two screens both
        // introducing the product at the same moment is worse than either alone, and Home
        // is one of the things the tour explains: arriving before it is explained is what
        // made the first minute confusing. TOUR_FINISHED_EVENT below opens it afterwards.
        if (!tourAlreadySeen()) return;
        homeShownRef.current = true;
        if (showHomeOnStartup) setShowHome(true);
    }, [showEngineSetup, showProfileSetup, showWorkspacePicker, workspacePathState, showHomeOnStartup]);

    // The tour has finished (or been skipped): now Home may open, once.
    useEffect(() => {
        const afterTour = () => {
            if (homeShownRef.current) return;
            homeShownRef.current = true;
            if (showHomeOnStartup) setShowHome(true);
        };
        window.addEventListener(TOUR_FINISHED_EVENT, afterTour);
        return () => window.removeEventListener(TOUR_FINISHED_EVENT, afterTour);
    }, [showHomeOnStartup]);

    const handleToggleShowHomeOnStartup = useCallback((next: boolean) => {
        setShowHomeOnStartup(next);
        savePersisted('showHomeOnStartup', next);
    }, []);
    const [newPipelineModal, setNewPipelineModal] = useState<{
        open: boolean;
        defaultParent: string;
    }>({ open: false, defaultParent: 'pipelines' });

    // Shown once after a legacy job file is translated, so an import that left
    // placeholders or unresolved credentials says so instead of looking done.
    const [importReport, setImportReport] = useState<ImportReport | null>(null);

    const activePipeline = pipelineData[activeJobId] ?? EMPTY_PIPELINE;
    const nodes = activePipeline.nodes;
    const edges = activePipeline.edges;

    // Web edition: no folder picker - the server tells us which workspace it
    // serves, so we adopt and load it.
    useEffect(() => {
        if (isInTauri() || !isWebBackend() || workspacePathState) return;
        let cancelled = false;
        invoke<{ workspace?: string }>('web_bootstrap')
            .then(b => {
                if (!cancelled && b?.workspace) setWorkspacePathState(b.workspace);
            })
            .catch(() => {});
        return () => {
            cancelled = true;
        };
    }, [workspacePathState]);

    // Hydrate from the workspace once the path is known (Tauri fs or web HTTP fs).
    useEffect(() => {
        if ((!isInTauri() && !isWebBackend()) || !workspacePathState) return;
        let cancelled = false;
        setWorkspaceLoadError(null);
        setCorruptFiles([]);
        loadWorkspace(workspacePathState)
            .then(async state => {
                if (cancelled) return;
                // Fresh / empty workspace on the desktop app: seed the bundled
                // sample pipelines and generate their data, then re-hydrate from
                // the files we just wrote. Best-effort - a seeding failure just
                // falls through to an empty workspace.
                if (!state && isInTauri()) {
                    let seeded = false;
                    try {
                        setSeeding(true);
                        seeded = await seedSampleWorkspace(workspacePathState);
                    } catch (err) {
                        console.error('Sample workspace seeding failed', err);
                    }
                    if (cancelled) return;
                    setSeeding(false);
                    if (seeded) {
                        setReloadNonce(n => n + 1);
                        return;
                    }
                    // Nothing loaded, and seeding declined. Seeding declines in
                    // exactly one case: the backend can see a duckle.json or a
                    // workspace.json that we just failed to read. (It reads
                    // through std::fs, which has no webview fs scope applied, so
                    // it sees folders the frontend may not be able to.) The
                    // folder is therefore a real workspace, not a fresh one.
                    //
                    // Falling through here would keep the bundled starter in
                    // memory, mark the workspace ready, and let auto-save write
                    // that starter over the user's duckle.json and
                    // repository.json. Stop instead and name the folder.
                    setWorkspaceLoadError(workspacePathState);
                    return;
                }
                if (state) {
                    if (state.engine) setEngine(normalizeEngineId(state.engine));
                    if (state.pipelineData)
                        setPipelineData(state.pipelineData as Record<string, PipelineState>);
                    if (state.repo)
                        setRepo(state.repo as RepoItem[]);
                    if (state.jobs && (state.jobs as Job[]).length > 0) {
                        const loadedJobs = state.jobs as Job[];
                        setJobs(loadedJobs);
                        const targetActiveId =
                            state.activeJobId && loadedJobs.some(j => j.id === state.activeJobId)
                                ? state.activeJobId
                                : loadedJobs[0].id;
                        setActiveJobId(targetActiveId);
                    } else {
                        // No pipelines in this workspace. Clearing matters on a
                        // workspace SWITCH: this effect re-runs in place for a
                        // new path (no remount, and workspaceReady is never set
                        // back to false), so the previous workspace's tabs would
                        // otherwise stay on screen - pointing at pipelineData
                        // that was just cleared - and auto-save would write them
                        // into the workspace that does not have them.
                        setJobs([]);
                        if (state.activeJobId) setActiveJobId(state.activeJobId);
                    }
                    if (state.corruptFiles?.length) setCorruptFiles(state.corruptFiles);
                }
                setWorkspaceReady(true);
                // #307: the palette learns what this workspace installs. Not
                // awaited and never fatal - an editor that would not open
                // because a components/ manifest is malformed would be absurd,
                // and the problems are reported where they can be acted on.
                if (workspacePathState) {
                    externalComponents(workspacePathState)
                        .then((r: { components: ComponentDef[]; problems: string[] }) => {
                            // The form each one declares, before the palette
                            // offers it: a tile you can drop but not configure
                            // is not a tile worth having.
                            for (const c of r.components as Array<
                                ComponentDef & { manifest?: unknown }
                            >) {
                                if (c.manifest) setExternalManifest(c.id, c.manifest);
                            }
                            setExternalComponents(r.components);
                            if (r.problems.length) console.warn('external components:', r.problems);
                        })
                        .catch(() => setExternalComponents([]));
                }
            })
            .catch(err => {
                if (cancelled) return;
                // Structural file corrupt: do NOT mark ready. Leaving
                // workspaceReady false keeps the auto-save effects disabled so
                // the in-memory defaults can never be written over the good
                // files on disk. Surface the exact file to fix or restore.
                const file =
                    err instanceof WorkspaceLoadError ? err.file : (err?.message ?? String(err));
                setWorkspaceLoadError(file);
                console.error('Workspace load failed (corrupt file)', err);
            });
        return () => {
            cancelled = true;
        };
    }, [workspacePathState, reloadNonce]);

    // Granular Tauri saves - each slice goes to its own file so the
    // workspace folder is git-friendly. Browser still uses localStorage.
    const prevPipelineDataRef = useRef<Record<string, PipelineState> | null>(null);
    const prevRepoRef = useRef<RepoItem[] | null>(null);

    useEffect(() => {
        if (!workspaceReady || (!isInTauri() && !isWebBackend()) || !workspacePathState) return;
        const ws = workspacePathState;
        const t = setTimeout(() => {
            void saveMetadata(ws, { engine, jobs, activeJobId });
        }, 200);
        return () => clearTimeout(t);
    }, [workspaceReady, workspacePathState, engine, jobs, activeJobId]);

    useEffect(() => {
        if (!workspaceReady || (!isInTauri() && !isWebBackend()) || !workspacePathState) return;
        const ws = workspacePathState;
        const t = setTimeout(() => {
            void (async () => {
                await saveRepository(ws, repo as unknown as Array<Record<string, unknown>>);
                // Diff payload-bearing items against the previous snapshot
                // so we only write the ones that actually changed.
                const prev = prevRepoRef.current ?? [];
                const prevById = new Map(prev.map(i => [i.id, i]));
                const currById = new Map(repo.map(i => [i.id, i]));
                let allSaved = true;
                for (const item of repo) {
                    if (
                        item.type === 'folder' ||
                        item.type === 'project' ||
                        item.type === 'pipeline'
                    )
                        continue;
                    const before = prevById.get(item.id);
                    if (!before || before.payload !== item.payload) {
                        if (item.payload !== undefined) {
                            const ok = await saveItemPayload(ws, item.type, item.id, item.payload);
                            if (!ok) allSaved = false;
                        }
                    }
                }
                // Delete payloads for items that were removed.
                for (const before of prev) {
                    if (currById.has(before.id)) continue;
                    if (before.type === 'pipeline') {
                        await deletePipelineFile(ws, before.id);
                    } else if (
                        before.type !== 'folder' &&
                        before.type !== 'project'
                    ) {
                        await deleteItemPayload(ws, before.type, before.id);
                    }
                }
                // Only advance the baseline if every payload persisted; a
                // failed item stays "changed" so the next debounce retries it.
                if (allSaved) {
                    prevRepoRef.current = repo;
                }
            })();
        }, 300);
        return () => clearTimeout(t);
    }, [workspaceReady, workspacePathState, repo]);

    useEffect(() => {
        if (!workspaceReady || (!isInTauri() && !isWebBackend()) || !workspacePathState) return;
        const ws = workspacePathState;
        const t = setTimeout(() => {
            void (async () => {
                const prev = prevPipelineDataRef.current ?? {};
                let allSaved = true;
                for (const [id, state] of Object.entries(pipelineData)) {
                    if (prev[id] !== state) {
                        const ok = await savePipelineFile(ws, id, state);
                        if (ok) {
                            // Clear the per-tab dirty flag only once it's really
                            // persisted (guard against a needless re-render).
                            setJobs(js =>
                                js.some(j => j.id === id && j.dirty)
                                    ? js.map(j => (j.id === id ? { ...j, dirty: false } : j))
                                    : js,
                            );
                        } else {
                            allSaved = false;
                        }
                    }
                }
                // Keep the baseline (and thus the dirty tabs) when a save failed
                // so the next debounce retries instead of dropping the edit.
                if (allSaved) {
                    prevPipelineDataRef.current = pipelineData;
                }
            })();
        }, 400);
        return () => clearTimeout(t);
    }, [workspaceReady, workspacePathState, pipelineData]);

    // Browser fallback: localStorage.
    //
    // The guard used to ask `isInTauri()`, which is false in the web edition, so the
    // web build fell through and wrote the repo to localStorage. Repo items carry
    // connection payloads that workspace.ts has already DECRYPTED, so every stored
    // credential ended up in the browser in clear text, readable by any script on the
    // origin and left behind on a shared machine. `hasBackend()` is the right
    // question: when a backend exists, Tauri or the web runner, it has already
    // persisted this server-side and localStorage is redundant.
    useEffect(() => {
        if (!workspaceReady) return;
        if (hasBackend() && workspacePathState) return;
        const t = setTimeout(() => {
            savePersisted('pipelines', pipelineData);
            // Belt and braces for the no-workspace case, which still reaches this
            // line: a decrypted connection payload must never reach localStorage.
            savePersisted(
                'repo',
                repo.map(item =>
                    item && (item as { type?: string }).type === 'connection'
                        ? { ...item, payload: undefined }
                        : item,
                ),
            );
            savePersisted('jobs', jobs);
            savePersisted('active-job', activeJobId);
            savePersisted('active-context', activeContextId);
            savePersisted('engine', engine);
        }, 250);
        return () => clearTimeout(t);
    }, [
        workspaceReady,
        workspacePathState,
        pipelineData,
        repo,
        jobs,
        activeJobId,
        activeContextId,
        engine,
    ]);

    // Ctrl/Cmd+S: flush the active pipeline + repo + metadata to disk now.
    // Saves are normally debounced; this makes the familiar gesture work
    // (and stops the webview's "save page" dialog from hijacking it).
    useEffect(() => {
        if (!isInTauri()) return;
        const onKey = (e: KeyboardEvent) => {
            if (!(e.ctrlKey || e.metaKey) || e.key.toLowerCase() !== 's') return;
            e.preventDefault();
            if (!workspacePathState) return;
            const ws = workspacePathState;
            void (async () => {
                const active = pipelineData[activeJobId];
                if (active) await savePipelineFile(ws, activeJobId, active);
                await saveRepository(ws, repo as unknown as Array<Record<string, unknown>>);
                await saveMetadata(ws, { engine, jobs, activeJobId });
                setJobs(js => js.map(j => (j.id === activeJobId ? { ...j, dirty: false } : j)));
            })();
        };
        window.addEventListener('keydown', onKey);
        return () => window.removeEventListener('keydown', onKey);
    }, [workspacePathState, pipelineData, activeJobId, repo, engine, jobs]);

    // Suppress the native webview right-click menu (Back / Reload / Print ...)
    // in the desktop app - it looks out of place on the header, footer and
    // chrome. Editable fields keep their native copy/paste menu, and the
    // canvas + Projects tree open their own context menus via their own
    // handlers (unaffected - this only kills the default where nothing else
    // handles the event).
    useEffect(() => {
        if (!isInTauri()) return;
        const onCtx = (e: MouseEvent) => {
            const t = e.target as HTMLElement | null;
            if (t && (t.isContentEditable || t.closest('input, textarea'))) return;
            e.preventDefault();
        };
        window.addEventListener('contextmenu', onCtx);
        return () => window.removeEventListener('contextmenu', onCtx);
    }, []);

    const handlePickedWorkspace = useCallback((path: string) => {
        setWorkspacePath(path);
        setWorkspacePathState(path);
    }, []);

    // Sync the workspace path with the Rust scheduler so it loads any
    // schedules persisted in that folder.
    useEffect(() => {
        if (!isInTauri()) return;
        void scheduleSetWorkspace(workspacePathState);
    }, [workspacePathState]);

    const [scheduleModalPipelineId, setScheduleModalPipelineId] = useState<string | null>(
        null,
    );
    const handleSchedulePipeline = useCallback((pipelineId: string) => {
        setScheduleModalPipelineId(pipelineId);
    }, []);

    const [backfillModalPipelineId, setBackfillModalPipelineId] = useState<string | null>(
        null,
    );
    const handleBackfillPipeline = useCallback((pipelineId: string) => {
        setBackfillModalPipelineId(pipelineId);
    }, []);

    // Run-parameters prompt (issue #127): names = the unbound ${...} the user
    // must value; target = run-to-here node id (null for a full run).
    const [runParamPrompt, setRunParamPrompt] = useState<{
        names: string[];
        target: string | null;
    } | null>(null);

    const [buildModalPipelineId, setBuildModalPipelineId] = useState<string | null>(null);
    const [showMcpModal, setShowMcpModal] = useState(false);
    const [showSettings, setShowSettings] = useState(false);
    const handleBuildPipeline = useCallback((pipelineId: string) => {
        setBuildModalPipelineId(pipelineId);
    }, []);

    const handleSwitchWorkspace = useCallback(async () => {
        if (!isInTauri()) return;
        const { pickWorkspaceDirectory } = await import('./workspace');
        const picked = await pickWorkspaceDirectory();
        if (!picked || picked === workspacePathState) return;
        // Reset state so loadWorkspace effect re-hydrates from the new
        // folder. We don't clear the existing state until we know the
        // new path is set, to avoid a flash of empty canvas.
        setWorkspaceReady(false);
        setPipelineData(INITIAL_PIPELINE_DATA);
        setRepo(INITIAL_REPO);
        setJobs(INITIAL_JOBS);
        setActiveJobId('j1');
        // Drop the save-diff baselines so the first save after the switch
        // diffs against the freshly loaded workspace, not the old one.
        prevPipelineDataRef.current = null;
        prevRepoRef.current = null;
        setWorkspacePath(picked);
        setWorkspacePathState(picked);
    }, [workspacePathState]);

    // #92: re-read the open workspace from disk, replacing in-memory state, so
    // external edits (MCP, git pull, hand-editing) are picked up - the files are
    // the source of truth. workspaceReady=false disables autosave so it can't
    // clobber during the reload; the load effect flips it back true on success.
    const handleReloadWorkspace = useCallback(() => {
        if ((!isInTauri() && !isWebBackend()) || !workspacePathState) return;
        setWorkspaceReady(false);
        prevPipelineDataRef.current = null;
        prevRepoRef.current = null;
        setReloadNonce(n => n + 1);
    }, [workspacePathState]);

    // Keep the active account pointed at whatever workspace is open.
    useEffect(() => {
        if (!activeAccountId || !workspacePathState) return;
        setAccounts(prev =>
            prev.map(a =>
                a.id === activeAccountId && a.workspacePath !== workspacePathState
                    ? { ...a, workspacePath: workspacePathState }
                    : a,
            ),
        );
    }, [workspacePathState, activeAccountId]);

    // Load an account's workspace context, reusing the workspace-switch reset
    // so the canvas re-hydrates cleanly (quick context switch).
    const loadAccountContext = useCallback(
        (path: string | null) => {
            // Already on this workspace: the in-memory state is already the
            // right one. Resetting to INITIAL here would blank the canvas down
            // to the default orders_etl sample and NOT re-hydrate, because the
            // loadWorkspace effect keys off workspacePathState (which wouldn't
            // change). Two accounts pointing at the same folder legitimately
            // share its data, so keep what's loaded.
            if (path === workspacePathState) return;
            setWorkspaceReady(false);
            setPipelineData(INITIAL_PIPELINE_DATA);
            setRepo(INITIAL_REPO);
            setJobs(INITIAL_JOBS);
            setActiveJobId('j1');
            prevPipelineDataRef.current = null;
            prevRepoRef.current = null;
            if (path) {
                setWorkspacePath(path);
                setWorkspacePathState(path);
            } else {
                setWorkspacePathState(null);
            }
        },
        [workspacePathState],
    );

    const handleCreateFirstAccount = useCallback(
        (v: { username: string; avatar?: string }) => {
            const acc: Account = {
                id: newAccountId(),
                username: v.username,
                avatar: v.avatar,
                workspacePath: workspacePathState ?? undefined,
            };
            setAccounts([acc]);
            setActiveAccountId(acc.id);
        },
        [workspacePathState],
    );

    const handleSwitchAccount = useCallback(
        (id: string) => {
            if (id === activeAccountId) return;
            const acc = accounts.find(a => a.id === id);
            setActiveAccountId(id);
            if (isInTauri()) loadAccountContext(acc?.workspacePath ?? null);
        },
        [accounts, activeAccountId, loadAccountContext],
    );

    const handleAddAccount = useCallback(
        (v: { username: string; avatar?: string }) => {
            const acc: Account = { id: newAccountId(), username: v.username, avatar: v.avatar };
            setAccounts(prev => [...prev, acc]);
            setActiveAccountId(acc.id);
            if (isInTauri()) loadAccountContext(null); // new account picks its own workspace
        },
        [loadAccountContext],
    );

    const handleEditAccount = useCallback(
        (id: string, v: { username: string; avatar?: string }) => {
            setAccounts(prev =>
                prev.map(a => (a.id === id ? { ...a, username: v.username, avatar: v.avatar } : a)),
            );
        },
        [],
    );

    const handleRemoveAccount = useCallback(
        (id: string) => {
            setAccounts(prev => {
                if (prev.length <= 1) return prev; // keep at least one account
                const next = prev.filter(a => a.id !== id);
                if (id === activeAccountId) {
                    const fallback = next[0];
                    setActiveAccountId(fallback.id);
                    if (isInTauri()) loadAccountContext(fallback.workspacePath ?? null);
                }
                return next;
            });
        },
        [activeAccountId, loadAccountContext],
    );

    const workspaceFolderName = useMemo(() => {
        if (!workspacePathState) return null;
        const parts = workspacePathState.split(/[\\/]/).filter(Boolean);
        return parts[parts.length - 1] ?? workspacePathState;
    }, [workspacePathState]);

    useEffect(() => {
        let cancelled = false;
        invoke<string>('ping')
            .then(reply => {
                if (!cancelled) setRuntime(reply === 'pong' ? 'ready' : 'offline');
            })
            .catch(() => {
                if (!cancelled) setRuntime('offline');
            });
        return () => {
            cancelled = true;
        };
    }, []);

    // Switching active pipeline resets node selection.
    useEffect(() => {
        setSelectedId(null);
    }, [activeJobId]);

    const updateActive = useCallback(
        (updater: (s: PipelineState) => PipelineState) => {
            setPipelineData(d => ({
                ...d,
                [activeJobId]: updater(d[activeJobId] ?? EMPTY_PIPELINE),
            }));
        },
        [activeJobId],
    );

    const setNodes = useCallback(
        (updater: Node<DuckleNodeData>[] | ((ns: Node<DuckleNodeData>[]) => Node<DuckleNodeData>[])) => {
            updateActive(s => ({
                ...s,
                nodes: typeof updater === 'function' ? (updater as (ns: Node<DuckleNodeData>[]) => Node<DuckleNodeData>[])(s.nodes) : updater,
            }));
        },
        [updateActive],
    );

    const setEdges = useCallback(
        (updater: Edge[] | ((es: Edge[]) => Edge[])) => {
            updateActive(s => ({
                ...s,
                edges: typeof updater === 'function' ? (updater as (es: Edge[]) => Edge[])(s.edges) : updater,
            }));
        },
        [updateActive],
    );

    const markDirty = useCallback(() => {
        setJobs(js => js.map(j => (j.id === activeJobId ? { ...j, dirty: true } : j)));
    }, [activeJobId]);

    // Undo/redo: restore a whole {nodes, edges} snapshot for the active
    // pipeline. The hook records history off `activePipeline` changes and
    // drives Ctrl+Z / Ctrl+Y / Ctrl+Shift+Z / Ctrl+R.
    const applyPipelineSnapshot = useCallback(
        (snapshot: CanvasSnapshot) => {
            setPipelineData(d => ({ ...d, [activeJobId]: snapshot as unknown as PipelineState }));
            markDirty();
        },
        [activeJobId, markDirty],
    );
    const { undo, redo } = useUndoRedo(
        activeJobId,
        activePipeline as unknown as CanvasSnapshot,
        applyPipelineSnapshot,
    );

    const handleNodesChange = useCallback(
        (changes: NodeChange[]) => {
            setNodes(ns => applyNodeChanges(changes, ns) as Node<DuckleNodeData>[]);
        },
        [setNodes],
    );

    const handleEdgesChange = useCallback(
        (changes: EdgeChange[]) => {
            setEdges(es => applyEdgeChanges(changes, es));
        },
        [setEdges],
    );

    const handleConnectWithType = useCallback(
        (connection: Connection, type: ConnectionType) => {
            setEdges(es =>
                addEdge(
                    {
                        ...connection,
                        type: 'duckle',
                        data: { connectionType: type },
                    },
                    es,
                ),
            );

            // Auto-populate the right-side key on join/lookup components
            // when a lookup connection lands on them - picks up the
            // first column of the lookup source's effective schema.
            if (type === 'lookup' && connection.target && connection.source) {
                const targetNode = nodes.find(n => n.id === connection.target);
                const targetManifest = targetNode
                    ? getManifest(targetNode.data.componentId)
                    : undefined;
                const targetId = targetManifest?.id ?? '';
                const isJoinFamily =
                    targetId.startsWith('xf.join.') ||
                    targetId === 'xf.lookup' ||
                    targetId === 'xf.semi' ||
                    targetId === 'xf.anti';
                if (isJoinFamily && targetNode && !targetNode.data.properties?.rightKey) {
                    const lookupSchema = resolveOutputSchema(connection.source, nodes, edges);
                    const firstCol = lookupSchema[0]?.name;
                    if (firstCol) {
                        setNodes(ns =>
                            ns.map(n =>
                                n.id === connection.target
                                    ? {
                                          ...n,
                                          data: {
                                              ...n.data,
                                              properties: {
                                                  ...(n.data.properties ?? {}),
                                                  rightKey: firstCol,
                                              },
                                          },
                                      }
                                    : n,
                            ),
                        );
                    }
                }
            }

            markDirty();
        },
        [nodes, edges, setNodes, setEdges, markDirty],
    );

    const handleEdgeChangeType = useCallback(
        (edgeId: string, newType: ConnectionType) => {
            setEdges(es =>
                es.map(e =>
                    e.id === edgeId
                        ? {
                              ...e,
                              type: 'duckle',
                              data: { ...(e.data ?? {}), connectionType: newType },
                          }
                        : e,
                ),
            );
            markDirty();
        },
        [setEdges, markDirty],
    );

    const handleEdgeDelete = useCallback(
        (edgeId: string) => {
            setEdges(es => es.filter(e => e.id !== edgeId));
            markDirty();
        },
        [setEdges, markDirty],
    );

    const [mapperNodeId, setMapperNodeId] = useState<string | null>(null);
    const mapperNode = useMemo(
        () => (mapperNodeId ? nodes.find(n => n.id === mapperNodeId) ?? null : null),
        [mapperNodeId, nodes],
    );
    const handleOpenMapper = useCallback((nodeId: string) => {
        setMapperNodeId(nodeId);
    }, []);
    const handleMapperSave = useCallback(
        (state: MapperState, derivedSchema: Column[]) => {
            if (!mapperNodeId) return;
            setNodes(ns =>
                ns.map(n => {
                    if (n.id !== mapperNodeId) return n;
                    // The engine reads join config from a top-level
                    // `lookups` property (not from inside `mapper`), so
                    // hoist it out; drop the key entirely when there are none.
                    const { lookups, ...mapperRest } = state;
                    const nextProps: Record<string, unknown> = {
                        ...(n.data.properties ?? {}),
                        mapper: mapperRest as unknown as Record<string, unknown>,
                        mode: 'visual',
                    };
                    // Visual mapper outputs are the single source of truth in
                    // visual mode; drop any stale key-value `expressions` so the
                    // engine (which prefers `expressions`) uses these outputs.
                    delete nextProps.expressions;
                    if (lookups && lookups.length) {
                        nextProps.lookups = lookups;
                    } else {
                        delete nextProps.lookups;
                    }
                    return {
                        ...n,
                        data: {
                            ...n.data,
                            properties: nextProps,
                            schema: derivedSchema,
                        },
                    };
                }),
            );
            setMapperNodeId(null);
            markDirty();
        },
        [mapperNodeId, setNodes, markDirty],
    );

    const [editingEdgeId, setEditingEdgeId] = useState<string | null>(null);
    const editingEdge = useMemo(
        () => (editingEdgeId ? edges.find(e => e.id === editingEdgeId) ?? null : null),
        [editingEdgeId, edges],
    );

    const handleEdgeEdit = useCallback((edgeId: string) => {
        setEditingEdgeId(edgeId);
    }, []);

    const handleEdgeEditSave = useCallback(
        (patch: { label?: string; condition?: string }) => {
            if (!editingEdgeId) return;
            setEdges(es =>
                es.map(e =>
                    e.id === editingEdgeId
                        ? {
                              ...e,
                              data: {
                                  ...(e.data ?? {}),
                                  ...(patch.label !== undefined ? { label: patch.label } : {}),
                                  ...(patch.condition !== undefined
                                      ? { condition: patch.condition }
                                      : {}),
                              },
                          }
                        : e,
                ),
            );
            setEditingEdgeId(null);
            markDirty();
        },
        [editingEdgeId, setEdges, markDirty],
    );

    const handleSelectionChange = useCallback((params: OnSelectionChangeParams) => {
        const sel = params.nodes[0] ?? null;
        setSelectedId(sel?.id ?? null);
        // Live mode: previewing whatever node you click makes the toggle feel
        // alive (run up to it + animate). Skip sinks (a run to a sink writes and
        // has no preview rows). Debounced so click-dragging a marquee selection
        // does not fire a run per intermediate selection.
        const componentId = (sel?.data as DuckleNodeData | undefined)?.componentId ?? '';
        if (liveModeRef.current && sel && !componentId.startsWith('snk.')) {
            if (liveTimerRef.current) clearTimeout(liveTimerRef.current);
            const target = sel.id;
            liveTimerRef.current = setTimeout(() => {
                liveTimerRef.current = null;
                triggerLivePreviewRef.current?.(target);
            }, 250);
        }
    }, []);

    const handleUpdateNode = useCallback(
        (id: string, patch: Partial<DuckleNodeData>) => {
            setNodes(ns =>
                ns.map(n => (n.id === id ? { ...n, data: { ...n.data, ...patch } } : n)),
            );
            markDirty();
            // Live mode: debounce a partial run up to the edited node. Only a
            // genuine user edit lands here - run results merge previews via
            // setNodes (not this handler), so a preview update can't re-trigger.
            if (liveModeRef.current) {
                if (liveTimerRef.current) clearTimeout(liveTimerRef.current);
                liveTimerRef.current = setTimeout(() => {
                    liveTimerRef.current = null;
                    triggerLivePreviewRef.current?.(id);
                }, 800);
            }
        },
        [setNodes, markDirty],
    );

    const selectedNode = useMemo(
        () => nodes.find(n => n.id === selectedId) ?? null,
        [nodes, selectedId],
    );

    const openNewPipelineModal = useCallback((parentId: string = 'pipelines') => {
        setNewPipelineModal({ open: true, defaultParent: parentId });
    }, []);

    const handleNewJob = useCallback(() => {
        openNewPipelineModal('pipelines');
    }, [openNewPipelineModal]);

    const handleCloseJob = useCallback(
        (id: string) => {
            setJobs(js => js.filter(j => j.id !== id));
            if (activeJobId === id) {
                const remaining = jobs.filter(j => j.id !== id);
                setActiveJobId(remaining[0]?.id ?? '');
            }
        },
        [activeJobId, jobs],
    );

    const [runResult, setRunResult] = useState<RunResult | null>(null);

    const handleEvent = useCallback(
        (evt: PipelineEvent) => {
            setRunResult(prev => {
                const next: RunResult = prev
                    ? { ...prev, nodes: { ...prev.nodes } }
                    : {
                          status: 'ok',
                          duration_ms: 0,
                          nodes: {},
                          preview: [],
                      };
                switch (evt.type) {
                    case 'started':
                        return { status: 'ok', duration_ms: 0, nodes: {}, preview: [], messages: [] };
                    case 'stage_started':
                        next.nodes[evt.node_id] = { status: 'running', kind: evt.kind };
                        break;
                    case 'stage_finished':
                        next.nodes[evt.node_id] = {
                            status: evt.status,
                            kind: evt.kind,
                            rows: evt.rows,
                            duration_ms: evt.duration_ms,
                            error: evt.error,
                        };
                        break;
                    case 'cancelled':
                        next.status = 'cancelled';
                        break;
                    case 'log':
                        next.messages = [
                            ...(next.messages ?? []),
                            { node_id: evt.node_id, level: evt.level, message: evt.message },
                        ];
                        break;
                    case 'finished':
                        next.status = evt.status;
                        next.duration_ms = evt.duration_ms;
                        break;
                }
                return next;
            });
        },
        [],
    );

    const finishRun = useCallback(
        (start: number, result: RunResult | null) => {
            if (result) {
                // The engine's RunResult has no messages; carry over the
                // log/warn lines accumulated from the streamed events.
                setRunResult(prev => {
                    // The Output and Console tabs iterate runResult.nodes in
                    // insertion order, but the engine serializes a BTreeMap
                    // (sorted by internal node id), which doesn't match execution
                    // order. The streamed stage_started events inserted prev.nodes
                    // keys in true execution order, so reorder the final result to
                    // follow that; any keys only in the result are appended
                    // (fallback for a run that didn't stream events). (#134)
                    const streamOrder = prev ? Object.keys(prev.nodes) : [];
                    const orderedNodes: RunResult['nodes'] = {};
                    for (const id of streamOrder) {
                        if (id in result.nodes) orderedNodes[id] = result.nodes[id];
                    }
                    for (const id of Object.keys(result.nodes)) {
                        if (!(id in orderedNodes)) orderedNodes[id] = result.nodes[id];
                    }
                    const ordered = { ...result, nodes: orderedNodes };
                    return prev?.messages?.length
                        ? { ...ordered, messages: prev.messages }
                        : ordered;
                });
                // Merge the previews back into each node's data so the
                // Preview tab and the inline schema badge stay in sync
                // with what just ran.
                if (result.preview.length > 0) {
                    const byId = new Map(result.preview.map(p => [p.node_id, p]));
                    setNodes(ns =>
                        ns.map(n => {
                            const p = byId.get(n.id);
                            if (!p) return n;
                            // A source node's schema is the user's declared input
                            // schema (set via Autodetect / the Schema panel) and the
                            // engine consumes it (e.g. CSV `types=`). A run must NOT
                            // overwrite it, or re-running keeps replacing a curated
                            // schema (issue #18). Keep an existing source schema and
                            // only refresh the preview rows; everything else (and a
                            // source with no schema yet) still takes the run columns.
                            const isSource = (n.data.componentId ?? '').startsWith('src.');
                            const keepSchema =
                                isSource && Array.isArray(n.data.schema) && n.data.schema.length > 0;
                            return {
                                ...n,
                                data: {
                                    ...n.data,
                                    schema: keepSchema ? n.data.schema : p.columns,
                                    sampleRows: p.rows,
                                },
                            };
                        }),
                    );
                }
            } else {
                setRunResult({
                    status: 'error',
                    duration_ms: Math.round(performance.now() - start),
                    nodes: {},
                    preview: [],
                    error:
                        'Pipeline execution is only available in the desktop app. Launch with `cargo run -p duckle-desktop`.',
                });
            }
        },
        [setNodes],
    );

    const validation = useMemo(
        () => validatePipeline(nodes, edges, repo),
        [nodes, edges, repo],
    );

    const [validateRequest, setValidateRequest] = useState<number>(0);
    const handleValidate = useCallback(() => {
        // Just bump a counter so BottomPanel pops the Problems tab.
        setValidateRequest(n => n + 1);
    }, []);

    // Shared run path for the Run and Run-from-here buttons. When the pipeline
    // references ${name} placeholders that no context or builtin resolves
    // (issue #127), prompt for those values first, then thread them through
    // resolveForRun (highest precedence) on both desktop and the web editor.
    // `target` is the run-to-here node, or null for a full run.
    const launchRun = useCallback(
        async (target: string | null, runtimeParams?: Record<string, string>) => {
            // Don't launch a run that's guaranteed to fail (e.g. a sink with no
            // output path) - that only yields a cryptic engine error. Surface
            // the Problems tab so the user can fix it first.
            if (validation.errorCount > 0) {
                setValidateRequest(n => n + 1);
                return;
            }
            const pipelineName = repo.find(r => r.id === activeJobId)?.name ?? activeJobId;
            // Global-context file vars load fresh each run so a runtime
            // KEY=VALUE file resolves too.
            const extra = await settingsLoadContextVars(workspacePathState ?? '');
            // First pass (no params supplied yet): if any ${name} is unbound,
            // pop the prompt and defer the run until the user submits.
            if (!runtimeParams) {
                const known = {
                    ...builtinVars(workspacePathState),
                    ...buildContextVars(repo),
                    ...extra,
                };
                const unbound = discoverParams(nodes, known);
                if (unbound.length > 0) {
                    setRunParamPrompt({ names: unbound, target });
                    return;
                }
            }
            setIsRunning(true);
            setRunResult(null);
            const start = performance.now();
            try {
                // Inline SQL routines + substitute ${context.var} (and the
                // supplied run params) before running; the canvas keeps the
                // editable, un-substituted values.
                const runNodes = resolveForRun(nodes, repo, workspacePathState, extra, runtimeParams);
                const result = target
                    ? await runPipelinePartial(
                          runNodes,
                          edges,
                          target,
                          handleEvent,
                          activeJobId,
                          workspacePathState,
                          pipelineName,
                      )
                    : await runPipeline(
                          runNodes,
                          edges,
                          handleEvent,
                          activeJobId,
                          workspacePathState,
                          pipelineName,
                      );
                finishRun(start, result);
            } finally {
                setIsRunning(false);
            }
        },
        [nodes, edges, repo, handleEvent, finishRun, activeJobId, workspacePathState, validation.errorCount],
    );

    const handleRun = useCallback(() => {
        void launchRun(null);
    }, [launchRun]);

    const handleRunFromHere = useCallback(
        (nodeId: string) => {
            void launchRun(nodeId);
        },
        [launchRun],
    );

    // Live-mode preview: a partial run up to the edited node, mirroring
    // handleRunFromHere but silent (no Problems-tab pop) and self-guarded so an
    // in-flight run is never stacked. Kept in a ref so the stable edit handler
    // always reaches the latest closure (fresh nodes/edges) at debounce time.
    const triggerLivePreview = useCallback(
        (nodeId: string) => {
            if (validation.errorCount > 0) return;
            if (isRunningRef.current) return;
            isRunningRef.current = true;
            setIsRunning(true);
            setRunResult(null);
            const start = performance.now();
            const pipelineName = repo.find(r => r.id === activeJobId)?.name ?? activeJobId;
            void settingsLoadContextVars(workspacePathState ?? '')
                .then(extra =>
                    runPipelinePartial(
                        resolveForRun(nodes, repo, workspacePathState, extra),
                        edges,
                        nodeId,
                        handleEvent,
                        activeJobId,
                        workspacePathState,
                        pipelineName,
                    ),
                )
                .then(result => finishRun(start, result))
                .finally(() => {
                    isRunningRef.current = false;
                    setIsRunning(false);
                });
        },
        [
            nodes,
            edges,
            repo,
            handleEvent,
            finishRun,
            activeJobId,
            workspacePathState,
            validation.errorCount,
        ],
    );
    useEffect(() => {
        triggerLivePreviewRef.current = triggerLivePreview;
    }, [triggerLivePreview]);

    const handleToggleLive = useCallback(() => {
        const turningOn = !liveModeRef.current;
        setLiveMode(turningOn);
        if (liveTimerRef.current) {
            clearTimeout(liveTimerRef.current);
            liveTimerRef.current = null;
        }
        // Turning live mode on should DO something immediately, not wait for the
        // next edit: preview the selected node right away (run up to it, with the
        // usual node animation). Skip sinks - running to a sink writes and yields
        // no preview rows. If nothing is selected, the next node you click or
        // edit triggers the preview instead.
        if (turningOn && selectedNode && !(selectedNode.data.componentId ?? '').startsWith('snk.')) {
            const target = selectedNode.id;
            liveTimerRef.current = setTimeout(() => {
                liveTimerRef.current = null;
                triggerLivePreviewRef.current?.(target);
            }, 50);
        }
    }, [selectedNode]);

    const handleStop = useCallback(() => {
        void cancelPipeline();
    }, []);

    const nodeLabels = useMemo(() => {
        const m: Record<string, string> = {};
        for (const n of nodes) m[n.id] = n.data.label;
        return m;
    }, [nodes]);

    // Which nodes' previews show in the Output panel: terminal nodes (the
    // pipeline's actual results) plus any Log Rows node, which prints its
    // rows there wherever it sits in the graph.
    const terminalNodeIds = useMemo(() => {
        const sources = new Set(edges.map(e => e.source));
        return nodes
            .filter(n => !sources.has(n.id) || n.data.componentId === 'xf.log')
            .map(n => n.id);
    }, [nodes, edges]);

    const handleSave = useCallback(() => {
        // Flush the active pipeline + repo + metadata to disk now, then clear
        // the tab's unsaved marker. (Autosave is debounced; the explicit
        // Save button / Ctrl+S gesture writes immediately.)
        setJobs(js => js.map(j => (j.id === activeJobId ? { ...j, dirty: false } : j)));
        if (!isInTauri() || !workspacePathState) return;
        const ws = workspacePathState;
        void (async () => {
            const active = pipelineData[activeJobId];
            if (active) await savePipelineFile(ws, activeJobId, active);
            await saveRepository(ws, repo as unknown as Array<Record<string, unknown>>);
            await saveMetadata(ws, { engine, jobs, activeJobId });
        })();
    }, [activeJobId, workspacePathState, pipelineData, repo, engine, jobs]);

    const activeJobName = useMemo(
        () => jobs.find(j => j.id === activeJobId)?.name ?? 'pipeline',
        [jobs, activeJobId],
    );

    const contexts = useMemo(() => repo.filter(r => r.type === 'context'), [repo]);

    const buildSqlText = useCallback(async (): Promise<string | null> => {
        // compilePipelineSql now throws the engine error on a compile
        // failure (so the Plan tab can show it). For copy/export we just
        // can't produce SQL in that case, so treat it as "nothing to do".
        try {
            const stages = await compilePipelineSql(nodes, edges);
            if (!stages) return null;
            return stages
                .map(
                    s =>
                        `-- ${s.kind.toUpperCase()} · ${s.label} (${s.node_id})\n${s.sql};\n`,
                )
                .join('\n');
        } catch (err) {
            console.warn('buildSqlText: pipeline does not compile', err);
            return null;
        }
    }, [nodes, edges]);

    const handleCopySql = useCallback(async () => {
        const text = await buildSqlText();
        if (!text) {
            await copyText(
                '-- SQL compilation requires the desktop app (cargo run -p duckle-desktop).',
            );
            return;
        }
        await copyText(text);
    }, [buildSqlText]);

    const handleExportSql = useCallback(async () => {
        const text = await buildSqlText();
        if (!text) return;
        await saveTextFile(`${activeJobName}.sql`, text, [
            { name: 'SQL', extensions: ['sql'] },
        ]);
    }, [buildSqlText, activeJobName]);

    const handleExportJson = useCallback(async () => {
        const payload = {
            version: 1,
            name: activeJobName,
            nodes,
            edges,
            exportedAt: new Date().toISOString(),
        };
        await saveTextFile(
            `${activeJobName}.duckle.json`,
            JSON.stringify(payload, null, 2),
            [{ name: 'Duckle pipeline', extensions: ['json'] }],
        );
    }, [nodes, edges, activeJobName]);

    const uniquePipelineName = useCallback(
        (base: string): string => {
            const taken = new Set(repo.filter(r => r.type === 'pipeline').map(r => r.name));
            if (!taken.has(base)) return base;
            for (let i = 2; i < 1000; i++) {
                const candidate = `${base}_${i}`;
                if (!taken.has(candidate)) return candidate;
            }
            return `${base}_${Date.now()}`;
        },
        [repo],
    );

    const importFromText = useCallback(
        (text: string, suggestedName: string) => {
            let parsed: { name?: string; nodes?: unknown; edges?: unknown };
            try {
                parsed = JSON.parse(text);
            } catch (err) {
                console.error('Pipeline import: invalid JSON', err);
                return;
            }
            const importedNodes = parsed.nodes;
            if (!Array.isArray(importedNodes) || importedNodes.length === 0) {
                console.error('Pipeline import: missing or empty nodes array');
                return;
            }
            const importedEdges = Array.isArray(parsed.edges) ? parsed.edges : [];
            const id = freshId('p');
            const baseName =
                (typeof parsed.name === 'string' && parsed.name.trim()) ||
                suggestedName.replace(/\.duckle\.json$|\.json$/, '') ||
                'imported_pipeline';
            const name = uniquePipelineName(baseName);
            const parent = repo.find(i => i.id === 'pipelines')
                ? 'pipelines'
                : repo.find(i => i.type === 'folder')?.id ?? 'root';
            setRepo(r => [...r, { id, name, type: 'pipeline', parentId: parent }]);
            setPipelineData(d => ({
                ...d,
                [id]: {
                    nodes: importedNodes as PipelineState['nodes'],
                    edges: importedEdges as PipelineState['edges'],
                },
            }));
            setJobs(js =>
                js.find(j => j.id === id) ? js : [...js, { id, name, dirty: false }],
            );
            setActiveJobId(id);
        },
        [repo],
    );

    const handleImportJson = useCallback(async () => {
        if (isInTauri()) {
            try {
                const { open } = await import('@tauri-apps/plugin-dialog');
                const { readTextFile } = await import('@tauri-apps/plugin-fs');
                const picked = await open({
                    multiple: false,
                    filters: [
                        { name: 'Duckle pipeline', extensions: ['json', 'duckle.json'] },
                        { name: 'All files', extensions: ['*'] },
                    ],
                });
                if (typeof picked !== 'string') return;
                const content = await readTextFile(picked);
                const filename = picked.split(/[\\/]/).pop() ?? 'imported.json';
                importFromText(content, filename);
            } catch (err) {
                console.error('Pipeline import (Tauri) failed', err);
            }
            return;
        }
        // Browser fallback - file input.
        const input = document.createElement('input');
        input.type = 'file';
        input.accept = '.json,.duckle.json,application/json';
        input.onchange = async () => {
            const file = input.files?.[0];
            if (!file) return;
            const content = await file.text();
            importFromText(content, file.name);
        };
        input.click();
    }, [importFromText]);

    // Import a legacy visual-ETL job file. Desktop only - the translation lives
    // in the Rust engine and `duckle serve` exposes no endpoint for it, so the
    // menu entry is hidden in the web editor rather than failing when clicked.
    const handleImportJob = useCallback(async () => {
        const { open } = await import('@tauri-apps/plugin-dialog');
        const picked = await open({
            multiple: false,
            filters: [
                { name: 'Job file', extensions: ['item'] },
                { name: 'All files', extensions: ['*'] },
            ],
        });
        if (typeof picked !== 'string') return;
        const jobName = picked.split(/[\\/]/).pop() ?? 'job';
        try {
            const result = await importJobFile(picked);
            if (result.nodeCount === 0) {
                // importFromText refuses an empty node list, so nothing would
                // open. Say that rather than reporting a successful import.
                setImportReport({
                    jobName,
                    nodeCount: 0,
                    translated: 0,
                    components: result.components,
                    warnings: result.warnings,
                    error: 'That job file holds no components, so there was nothing to open.',
                });
                return;
            }
            importFromText(JSON.stringify(result.pipeline), jobName);
            setImportReport({
                jobName,
                nodeCount: result.nodeCount,
                translated: result.translated,
                components: result.components,
                warnings: result.warnings,
            });
        } catch (err) {
            const message =
                typeof err === 'string' ? err : err instanceof Error ? err.message : String(err);
            setImportReport({
                jobName,
                nodeCount: 0,
                translated: 0,
                components: [],
                warnings: [],
                error: message,
            });
        }
    }, [importFromText]);

    const handleAutoLayout = useCallback(() => {
        setNodes(ns => layoutByDependency(ns, edges));
        markDirty();
    }, [setNodes, edges, markDirty]);

    const handleDropComponent = useCallback(
        (component: ComponentDef, position: DropPosition) => {
            const id = freshId('n');
            const manifest = getManifest(component.id);
            const flowType = paletteKindToFlowType(component.kind);
            const newNode: Node<DuckleNodeData> = {
                id,
                type: flowType,
                position,
                data: {
                    // No static subtitle - the canvas derives it live from
                    // the component's config (file name, predicate, keys…).
                    label: component.label,
                    componentId: component.id,
                    properties: manifest ? getDefaults(manifest) : {},
                },
            };
            setNodes(ns => [...ns, newNode]);
            setSelectedId(id);
            markDirty();

            // Auto-detect schema for sources / autodetect-capable components
            // so downstream nodes inherit immediately. The mock returns sample
            // columns; real autodetect lands when the runtime can read files.
            if (manifest?.autodetect) {
                manifest
                    .autodetect(newNode.data.properties ?? {})
                    .then(result => {
                        setNodes(ns =>
                            ns.map(n =>
                                n.id === id
                                    ? {
                                          ...n,
                                          data: {
                                              ...n.data,
                                              schema: result.columns,
                                              sampleRows: result.sampleRows,
                                          },
                                      }
                                    : n,
                            ),
                        );
                    })
                    .catch(() => {
                        // Best-effort on add; a freshly added node often has
                        // unresolved / empty props. The Schema tab's Autodetect
                        // surfaces the real error instead (issue #148).
                    });
            }
        },
        [setNodes, markDirty],
    );

    const nodeAutodetectAvailable = useCallback(
        (nodeId: string) => {
            const node = nodes.find(n => n.id === nodeId);
            if (!node) return false;
            const manifest = getManifest(node.data.componentId);
            return Boolean(manifest?.autodetect);
        },
        [nodes],
    );

    const handleNodeAction = useCallback(
        (action: NodeAction, nodeId: string) => {
            const node = nodes.find(n => n.id === nodeId);
            if (!node) return;

            switch (action) {
                case 'rename':
                    setSelectedId(nodeId);
                    setRenameRequest(n => n + 1);
                    break;

                case 'duplicate': {
                    const dupId = freshId('n');
                    const copy: Node<DuckleNodeData> = {
                        ...node,
                        id: dupId,
                        position: { x: node.position.x + 40, y: node.position.y + 40 },
                        data: { ...node.data, label: node.data.label + ' (copy)' },
                        selected: false,
                    };
                    setNodes(ns => [...ns, copy]);
                    setSelectedId(dupId);
                    markDirty();
                    break;
                }

                case 'copy': {
                    // Copy the whole selection when this node is part of it,
                    // otherwise just this node. Paste lands in any pipeline.
                    const selected = nodes.filter(n => n.selected);
                    const toCopy = selected.some(n => n.id === nodeId) ? selected : [node];
                    writeClipboard(toCopy, edges);
                    break;
                }

                case 'toggle-disable':
                    setNodes(ns =>
                        ns.map(n =>
                            n.id === nodeId
                                ? {
                                      ...n,
                                      data: { ...n.data, disabled: !n.data.disabled },
                                  }
                                : n,
                        ),
                    );
                    markDirty();
                    break;

                case 'autodetect': {
                    const manifest = getManifest(node.data.componentId);
                    if (!manifest?.autodetect) return;
                    manifest
                        .autodetect(node.data.properties ?? {})
                        .then(result => {
                            setNodes(ns =>
                                ns.map(n =>
                                    n.id === nodeId
                                        ? {
                                              ...n,
                                              data: {
                                                  ...n.data,
                                                  schema: result.columns,
                                                  sampleRows: result.sampleRows,
                                              },
                                          }
                                        : n,
                                ),
                            );
                            markDirty();
                        })
                        .catch(err => {
                            // Open the node's Schema tab to run Autodetect there,
                            // where the real error is shown (issue #148).
                            console.warn('autodetect failed', err);
                        });
                    break;
                }

                case 'run-from-here':
                    handleRunFromHere(nodeId);
                    break;

                case 'copy-id':
                    void copyText(nodeId);
                    break;

                case 'delete':
                    setNodes(ns => ns.filter(n => n.id !== nodeId));
                    setEdges(es => es.filter(e => e.source !== nodeId && e.target !== nodeId));
                    if (selectedId === nodeId) setSelectedId(null);
                    markDirty();
                    break;
            }
        },
        [nodes, edges, selectedId, setNodes, setEdges, markDirty, handleRunFromHere],
    );

    // Copy the selected component(s) - falling back to the single active
    // selection - plus their internal wiring to the cross-pipeline clipboard.
    const handleCopy = useCallback(() => {
        const selected = nodes.filter(n => n.selected);
        const toCopy = selected.length > 0 ? selected : nodes.filter(n => n.id === selectedId);
        writeClipboard(toCopy, edges);
    }, [nodes, edges, selectedId]);

    // Paste clipboard components into the active pipeline with fresh ids and a
    // small offset, selecting them so the paste is obvious.
    const handlePaste = useCallback(() => {
        const clip = readClipboard();
        if (!clip) return;
        const { nodes: newNodes, edges: newEdges } = instantiateClipboard(
            clip,
            () => freshId('n'),
            () => freshId('e'),
        );
        setNodes(ns => [...ns.map(n => (n.selected ? { ...n, selected: false } : n)), ...newNodes]);
        if (newEdges.length > 0) setEdges(es => [...es, ...newEdges]);
        setSelectedId(newNodes[0]?.id ?? null);
        markDirty();
    }, [setNodes, setEdges, markDirty]);

    // Ctrl/Cmd+C / Ctrl/Cmd+V: copy and paste the selected canvas component(s).
    // Skips when the focus is in a text field (so editing properties still
    // copies text) and when the user has a real text selection.
    useEffect(() => {
        const onKey = (e: KeyboardEvent) => {
            if (!(e.ctrlKey || e.metaKey)) return;
            const k = e.key.toLowerCase();
            if (k !== 'c' && k !== 'v') return;
            const t = e.target as HTMLElement | null;
            const tag = t?.tagName;
            if (t?.isContentEditable || tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT') {
                return;
            }
            if (k === 'c') {
                if ((window.getSelection()?.toString() ?? '') !== '') return;
                handleCopy();
            } else {
                e.preventDefault();
                handlePaste();
            }
        };
        window.addEventListener('keydown', onKey);
        return () => window.removeEventListener('keydown', onKey);
    }, [handleCopy, handlePaste]);

    const handlePaneAction = useCallback(
        (action: PaneAction) => {
            switch (action) {
                case 'auto-layout':
                    handleAutoLayout();
                    break;
                case 'select-all':
                    setNodes(ns => ns.map(n => ({ ...n, selected: true })));
                    break;
                case 'undo':
                    undo();
                    break;
                case 'redo':
                    redo();
                    break;
                case 'paste':
                    handlePaste();
                    break;
                case 'build':
                    handleBuildPipeline(activeJobId);
                    break;
            }
        },
        [handleAutoLayout, setNodes, undo, redo, handlePaste, handleBuildPipeline, activeJobId],
    );

    // Repository handlers ---------------------------------------------------
    const handleOpenPipeline = useCallback(
        (id: string) => {
            const item = repo.find(i => i.id === id);
            if (!item || item.type !== 'pipeline') return;
            setJobs(js =>
                js.find(j => j.id === id) ? js : [...js, { id, name: item.name, dirty: false }],
            );
            setPipelineData(d => (d[id] ? d : { ...d, [id]: EMPTY_PIPELINE }));
            setActiveJobId(id);
        },
        [repo],
    );

    const handleNewFolderInRepo = useCallback(
        (parentId: string) => {
            const id = freshId('f');
            const count = repo.filter(i => i.type === 'folder' && i.parentId === parentId).length;
            const name = 'new_folder' + (count > 0 ? '_' + (count + 1) : '');
            const realParent = repo.find(
                i => i.id === parentId && (i.type === 'folder' || i.type === 'project'),
            )
                ? parentId
                : 'root';
            setRepo(r => [...r, { id, name, type: 'folder', parentId: realParent }]);
        },
        [repo],
    );

    const handleRenameRepoItem = useCallback((id: string, newName: string) => {
        setRepo(r => r.map(i => (i.id === id ? { ...i, name: newName } : i)));
        setJobs(js => js.map(j => (j.id === id ? { ...j, name: newName } : j)));
    }, []);

    const handleMoveRepoItem = useCallback(
        (id: string, newParentId: string) => {
            const item = repo.find(i => i.id === id);
            const target = repo.find(i => i.id === newParentId);
            if (!item || !target || id === newParentId) return;
            if (item.type === 'project') return;
            if (target.type !== 'folder' && target.type !== 'project') return;
            if (item.parentId === newParentId) return;
            // Reject moving a container into its own subtree (would orphan it).
            let cur: typeof target | undefined = target;
            while (cur) {
                if (cur.id === id) return;
                cur = cur.parentId ? repo.find(i => i.id === cur!.parentId) : undefined;
            }
            setRepo(r => r.map(i => (i.id === id ? { ...i, parentId: newParentId } : i)));
        },
        [repo],
    );

    const handleDuplicateRepoItem = useCallback(
        (id: string) => {
            const item = repo.find(i => i.id === id);
            if (!item) return;
            const newId = freshId(item.type[0]);
            setRepo(r => [...r, { ...item, id: newId, name: item.name + '_copy' }]);
            if (item.type === 'pipeline') {
                setPipelineData(d => ({ ...d, [newId]: d[id] ?? EMPTY_PIPELINE }));
            }
        },
        [repo],
    );

    const handleDeleteRepoItem = useCallback(
        (id: string) => {
            const item = repo.find(i => i.id === id);
            if (!item || item.type === 'project') return;
            const toDelete = new Set<string>([id]);
            const addDescendants = (parentId: string) => {
                for (const c of repo) {
                    if (c.parentId === parentId) {
                        toDelete.add(c.id);
                        addDescendants(c.id);
                    }
                }
            };
            addDescendants(id);
            setRepo(r => r.filter(i => !toDelete.has(i.id)));
            setJobs(js => js.filter(j => !toDelete.has(j.id)));
            setPipelineData(d => {
                const next = { ...d };
                for (const did of toDelete) delete next[did];
                return next;
            });
            if (toDelete.has(activeJobId)) {
                const remaining = jobs.filter(j => !toDelete.has(j.id));
                setActiveJobId(remaining[0]?.id ?? '');
            }
        },
        [repo, jobs, activeJobId],
    );

    // #208: gate deletion behind a confirmation. The context menu / Del key now
    // request a delete (open the dialog) instead of mutating state immediately;
    // handleDeleteRepoItem above is the actual delete, called only on confirm.
    const [pendingDelete, setPendingDelete] = useState<RepoItem | null>(null);
    const requestDeleteRepoItem = useCallback(
        (id: string) => {
            const item = repo.find(i => i.id === id);
            if (!item || item.type === 'project') return;
            setPendingDelete(item);
        },
        [repo],
    );
    const describeDelete = (item: RepoItem): string => {
        const ids = new Set<string>([item.id]);
        const addKids = (pid: string) => {
            for (const c of repo)
                if (c.parentId === pid) {
                    ids.add(c.id);
                    addKids(c.id);
                }
        };
        addKids(item.id);
        const n = ids.size - 1;
        const nested = n > 0 ? ` and its ${n} nested item${n === 1 ? '' : 's'}` : '';
        return `Delete ${item.type} "${item.name}"${nested}?\n\nThis cannot be undone.`;
    };

    const handleCreatePipeline = useCallback(
        (rawName: string, parentId: string, template: PipelineTemplate) => {
            const id = freshId('p');
            const realParent = repo.find(
                i => i.id === parentId && (i.type === 'folder' || i.type === 'project'),
            )
                ? parentId
                : 'pipelines';
            const seed = seedTemplate(template);
            setRepo(r => [...r, { id, name: rawName, type: 'pipeline', parentId: realParent }]);
            setPipelineData(d => ({ ...d, [id]: seed }));
            setJobs(js => [...js, { id, name: rawName, dirty: false }]);
            setActiveJobId(id);
            setNewPipelineModal({ open: false, defaultParent: 'pipelines' });
        },
        [repo],
    );

    // Map a component_id prefix to the React Flow node "kind" the
    // canvas understands. Mirrors how SAMPLE_NODES classifies new
    // tiles when a user drags from the palette.
    const nodeKindFromComponent = (componentId: string): string => {
        if (componentId.startsWith('src.')) return 'source';
        if (componentId.startsWith('snk.')) return 'sink';
        if (componentId.startsWith('ctl.')) return 'control';
        if (componentId.startsWith('qa.')) return 'transform';
        if (componentId.startsWith('code.')) return 'transform';
        if (componentId.startsWith('xf.')) return 'transform';
        return 'transform';
    };

    // Convert an AI-generated pipeline JSON (from chat) into the
    // canvas's PipelineState shape and replace the current pipeline's
    // content. Auto-lays nodes out left-to-right since the model
    // doesn't ship coordinates. Falls back to a no-op if the JSON
    // doesn't validate.
    const handleInsertAiPipeline = useCallback(
        (raw: unknown) => {
            if (!raw || typeof raw !== 'object') return;
            const obj = raw as {
                nodes?: Array<{ id?: string; type?: string; data?: { label?: string; properties?: unknown; props?: unknown } }>;
                edges?: Array<{ id?: string; source?: string; target?: string }>;
            };
            if (!Array.isArray(obj.nodes)) return;
            // Normalize the component ids the local model most often gets
            // wrong, so a near-miss resolves to a real node instead of an
            // empty / unknown one.
            const AI_ID_ALIASES: Record<string, string> = {
                'xf.aggregate': 'xf.groupby',
                'xf.derive': 'xf.addcol',
                'xf.select': 'xf.project',
                'xf.join': 'xf.join.inner',
            };
            const nodes: Node<DuckleNodeData>[] = obj.nodes.map((n, i) => {
                const cid = AI_ID_ALIASES[n.type ?? ''] ?? n.type ?? 'src.csv';
                return {
                    id: n.id ?? `n${i + 1}`,
                    type: nodeKindFromComponent(cid),
                    position: { x: 80 + i * 260, y: 160 },
                    data: {
                        label: n.data?.label ?? cid.replace(/^[^.]+\./, ''),
                        componentId: cid,
                        // The local model is told to emit `properties`, but older
                        // prompt builds said `props` - accept either so its output
                        // is not silently dropped into an empty node (issue: AI
                        // assistant produced empty aggregation nodes).
                        properties: ((n.data?.properties ?? n.data?.props) as Record<string, unknown> | undefined) ?? {},
                    } as DuckleNodeData,
                };
            });
            const edges: Edge[] = (obj.edges ?? []).map((e, i) => ({
                id: e.id ?? `e${i + 1}`,
                source: e.source ?? '',
                target: e.target ?? '',
                sourceHandle: 'main',
                targetHandle: 'main',
                type: 'duckle',
            }));
            setPipelineData(d => ({ ...d, [activeJobId]: { nodes, edges } }));
            setJobs(js => js.map(j => (j.id === activeJobId ? { ...j, dirty: true } : j)));
        },
        [activeJobId],
    );

    // Repo-item editor modal state (connections / contexts / docs / routines)
    type EditorState =
        | { kind: 'connection'; itemId: string | null; parentId: string }
        | { kind: 'context'; itemId: string | null; parentId: string }
        | { kind: 'document'; itemId: string | null; parentId: string }
        | { kind: 'routine'; itemId: string | null; parentId: string }
        | { kind: 'dive'; itemId: string | null; parentId: string }
        | { kind: 'dashboard'; itemId: string | null; parentId: string }
        | null;
    const [repoEditor, setRepoEditor] = useState<EditorState>(null);

    const handleNewConnection = useCallback(
        (parentId: string) => setRepoEditor({ kind: 'connection', itemId: null, parentId }),
        [],
    );
    const handleNewContext = useCallback(
        (parentId: string) => setRepoEditor({ kind: 'context', itemId: null, parentId }),
        [],
    );
    const handleNewDocument = useCallback(
        (parentId: string) => setRepoEditor({ kind: 'document', itemId: null, parentId }),
        [],
    );
    const handleNewRoutine = useCallback(
        (parentId: string) => setRepoEditor({ kind: 'routine', itemId: null, parentId }),
        [],
    );
    const handleNewDive = useCallback(
        (parentId: string) => setRepoEditor({ kind: 'dive', itemId: null, parentId }),
        [],
    );
    const handleNewDashboard = useCallback(
        (parentId: string) => setRepoEditor({ kind: 'dashboard', itemId: null, parentId }),
        [],
    );

    const handleOpenRepoItem = useCallback((item: RepoItem) => {
        if (item.type === 'connection')
            setRepoEditor({
                kind: 'connection',
                itemId: item.id,
                parentId: item.parentId ?? 'connections',
            });
        else if (item.type === 'context')
            setRepoEditor({
                kind: 'context',
                itemId: item.id,
                parentId: item.parentId ?? 'contexts',
            });
        else if (item.type === 'doc')
            setRepoEditor({
                kind: 'document',
                itemId: item.id,
                parentId: item.parentId ?? 'docs',
            });
        else if (item.type === 'routine')
            setRepoEditor({
                kind: 'routine',
                itemId: item.id,
                parentId: item.parentId ?? 'routines',
            });
        else if (item.type === 'dive')
            setRepoEditor({
                kind: 'dive',
                itemId: item.id,
                parentId: item.parentId ?? 'dives',
            });
        else if (item.type === 'dashboard')
            setRepoEditor({
                kind: 'dashboard',
                itemId: item.id,
                parentId: item.parentId ?? 'dashboards',
            });
    }, []);

    const editingRepoItem = useMemo(
        () => (repoEditor?.itemId ? repo.find(i => i.id === repoEditor.itemId) ?? null : null),
        [repoEditor, repo],
    );

    const upsertRepoItem = useCallback(
        (
            type: 'connection' | 'context' | 'doc' | 'routine' | 'dive' | 'dashboard',
            name: string,
            payload: unknown,
        ) => {
            if (!repoEditor) return;
            if (repoEditor.itemId) {
                setRepo(r =>
                    r.map(i =>
                        i.id === repoEditor.itemId
                            ? { ...i, name, payload: payload as RepoItem['payload'] }
                            : i,
                    ),
                );
            } else {
                const id =
                    type[0] +
                    '_' +
                    Date.now().toString(36) +
                    '_' +
                    Math.random().toString(36).slice(2, 6);
                setRepo(r => [
                    ...r,
                    {
                        id,
                        name,
                        type,
                        parentId: repoEditor.parentId,
                        payload: payload as RepoItem['payload'],
                    },
                ]);
            }
            setRepoEditor(null);
        },
        [repoEditor],
    );

    const handleSaveConnection = useCallback(
        (name: string, payload: ConnectionPayload) => upsertRepoItem('connection', name, payload),
        [upsertRepoItem],
    );
    const handleSaveContext = useCallback(
        (name: string, payload: ContextPayload) => upsertRepoItem('context', name, payload),
        [upsertRepoItem],
    );
    const handleSaveDocument = useCallback(
        (name: string, payload: DocumentPayload) => upsertRepoItem('doc', name, payload),
        [upsertRepoItem],
    );
    const handleSaveRoutine = useCallback(
        (name: string, payload: RoutinePayload) => upsertRepoItem('routine', name, payload),
        [upsertRepoItem],
    );
    const handleSaveDive = useCallback(
        (name: string, payload: Dive) => upsertRepoItem('dive', name, payload),
        [upsertRepoItem],
    );
    const handleSaveDashboard = useCallback(
        (name: string, payload: Dashboard) => upsertRepoItem('dashboard', name, payload),
        [upsertRepoItem],
    );
    // #105: the Plan tab compiles the RESOLVED nodes (context vars / ${...}
    // placeholders substituted, like a run does), so the preview matches what
    // executes instead of showing raw ${VAR} the run would have replaced. The
    // canvas keeps the un-substituted, editable `nodes`.
    const planNodes = useMemo(
        () => resolveForRun(nodes, repo, workspacePathState),
        [nodes, repo, workspacePathState],
    );
    const diveItems = useMemo(() => repo.filter((r) => r.type === 'dive'), [repo]);
    const dashboardItems = useMemo(() => repo.filter((r) => r.type === 'dashboard'), [repo]);
    const [showDivesGallery, setShowDivesGallery] = useState(false);
    // Settings toggle to hide the top-bar Dives button. Persisted locally and
    // applied live via a window event when changed in the Settings modal.
    const [hideDivesButton, setHideDivesButton] = useState(() => loadPersisted('hideDivesButton', false));
    useEffect(() => {
        const onChange = () => setHideDivesButton(loadPersisted('hideDivesButton', false));
        window.addEventListener('duckle:dives-visibility', onChange);
        return () => window.removeEventListener('duckle:dives-visibility', onChange);
    }, []);
    const [showLineage, setShowLineage] = useState(false);
    const [showTrust, setShowTrust] = useState(false);
    const [showCatalog, setShowCatalog] = useState(false);
    const [showPlans, setShowPlans] = useState(false);
    const [deployModalPipelineId, setDeployModalPipelineId] = useState<string | null>(null);

    const openJobIds = useMemo(() => new Set(jobs.map(j => j.id)), [jobs]);

    // Note: double-click-to-maximize on the title bar is handled natively
    // by Tauri's `data-tauri-drag-region`. A custom onDoubleClick handler
    // here would call toggleMaximize() a SECOND time, so the window
    // maximized then immediately restored. Do not re-add one.

    return (
        <RunStatusContext.Provider value={runResult?.nodes ?? {}}>
        <div className="app">
            <WindowResizeHandles />
            <header
                className="topbar"
                data-tauri-drag-region
            >
                <div className="brand" data-tauri-drag-region>
                    <DuckleLogo size={26} className="brand-logo" />
                    <span className="brand-text">
                        <span className="brand-name">Duckle</span>
                        <span className="brand-by">by SlothFlowLabs</span>
                    </span>
                </div>
                <div className="topbar-sep" aria-hidden="true" />
                <EngineSelector value={engine} onChange={setEngine} />
                <div className="topbar-spacer" data-tauri-drag-region />
                {workspaceFolderName ? (
                    <button
                        type="button"
                        className="topbar-workspace"
                        onClick={handleSwitchWorkspace}
                        title={t('topbar.workspaceTooltip', { path: workspacePathState })}
                    >
                        <FolderOpen size={12} />
                        <span className="topbar-workspace-name">{workspaceFolderName}</span>
                    </button>
                ) : null}
                {workspaceFolderName ? (
                    <button
                        type="button"
                        className="topbar-workspace"
                        onClick={handleReloadWorkspace}
                        title="Reload workspace from disk (pick up external edits)"
                    >
                        <RotateCw size={12} />
                    </button>
                ) : null}
                {contexts.length > 0 ? (
                    <div
                        className="topbar-context"
                        data-tour="context"
                        title={t('topbar.activeContextHint')}
                    >
                        <Braces size={12} aria-hidden="true" />
                        <select
                            className="topbar-context-select"
                            value={activeContextId ?? ''}
                            onChange={e => setActiveContextId(e.target.value || null)}
                            aria-label={t('topbar.activeContextHint')}
                        >
                            <option value="">{t('topbar.noContext')}</option>
                            {contexts.map(c => (
                                <option key={c.id} value={c.id}>
                                    {c.name}
                                </option>
                            ))}
                        </select>
                    </div>
                ) : null}
                {workspacePathState && isInTauri() ? (
                    <button
                        type="button"
                        className="topbar-theme-toggle dashboard-glow-btn"
                        data-tour="dashboard"
                        onClick={async () => {
                            try {
                                const url = await invoke<string>('open_web_panel', {
                                    workspace: workspacePathState,
                                });
                                await openExternal(url);
                            } catch (e) {
                                alert('Could not open the web panel: ' + String(e));
                            }
                        }}
                        title="Open the web dashboard (run + monitor pipelines in a browser)"
                        aria-label="Open web dashboard"
                    >
                        <LayoutDashboard size={14} className="dashboard-icon-glow" />
                    </button>
                ) : null}
                {!hideDivesButton ? (
                    <button
                        type="button"
                        className="topbar-theme-toggle"
                        data-tour="dives"
                        onClick={() => setShowDivesGallery(true)}
                        title="Dives - live data views & dashboards"
                        aria-label="Open dives"
                    >
                        <BarChart3 size={14} />
                    </button>
                ) : null}
                <button
                    type="button"
                    className="topbar-theme-toggle"
                    data-tour="lineage"
                    onClick={() => setShowLineage(true)}
                    title="Column lineage - trace each column back to its sources"
                    aria-label="Open column lineage"
                >
                    <Waypoints size={14} />
                </button>
                <button
                    type="button"
                    className="topbar-theme-toggle"
                    data-tour="trust"
                    onClick={() => setShowTrust(true)}
                    title="Trust score - an explainable score for this pipeline"
                    aria-label="Open trust score"
                >
                    <ShieldCheck size={14} />
                </button>
                <button
                    type="button"
                    className="topbar-theme-toggle"
                    data-tour="topbar"
                    onClick={() => setShowMcpModal(true)}
                    title="Connect to Claude"
                    aria-label="Connect Duckle to Claude"
                >
                    <ClaudeIcon size={14} className="claude-icon claude-icon-glow" />
                </button>
                <button
                    type="button"
                    className="topbar-theme-toggle"
                    data-tour="settings"
                    onClick={() => setShowSettings(true)}
                    title="Settings (proxy)"
                    aria-label="Open settings"
                >
                    <SettingsIcon size={14} />
                </button>
                <button
                    type="button"
                    className="topbar-theme-toggle"
                    data-tour="git"
                    onClick={() => setShowGitPanel(s => !s)}
                    title={t('topbar.git')}
                    aria-label={t('topbar.gitAriaToggle')}
                    aria-pressed={showGitPanel}
                    disabled={!workspacePathState}
                >
                    <GitBranch size={14} />
                </button>
                <button
                    type="button"
                    className="topbar-theme-toggle"
                    data-tour="duckie"
                    onClick={() => setShowChatPanel(s => !s)}
                    title={t('topbar.duckieAssistant')}
                    aria-label={t('topbar.duckieAssistantAriaToggle')}
                    aria-pressed={showChatPanel}
                >
                    <Sparkles size={14} />
                </button>
                <LanguageSelector />
                {accounts.length > 0 && activeAccount ? (
                    <AccountChip
                        accounts={accounts}
                        activeId={activeAccount.id}
                        onSwitch={handleSwitchAccount}
                        onAdd={handleAddAccount}
                        onEdit={handleEditAccount}
                        onRemove={handleRemoveAccount}
                    />
                ) : null}
                <button
                    type="button"
                    className="topbar-theme-toggle"
                    onClick={toggleTheme}
                    title={theme === 'dark' ? t('topbar.switchLight') : t('topbar.switchDark')}
                    aria-label={t('topbar.themeAriaToggle')}
                >
                    {theme === 'dark' ? <Sun size={14} /> : <Moon size={14} />}
                </button>
                <WindowControls />
            </header>

            <UpdateBanner />
            <EngineUpgradeBanner />
            <ReviewPrompt />

            {workspaceLoadError ? (
                <div className="update-banner is-error" role="alert">
                    <span className="update-banner-icon" aria-hidden="true">
                        ⚠
                    </span>
                    <span className="update-banner-text">
                        {t('workspace.loadError', {
                            file: workspaceLoadError,
                            defaultValue:
                                'Could not open this workspace: {{file}} contains invalid JSON. Fix or restore that file, then reload. Editing is paused so your other files stay safe.',
                        })}
                    </span>
                    <button
                        type="button"
                        className="update-banner-cta"
                        onClick={() => window.location.reload()}
                    >
                        {t('workspace.reload', 'Reload')}
                    </button>
                </div>
            ) : corruptFiles.length ? (
                <div className="update-banner is-warn" role="alert">
                    <span className="update-banner-icon" aria-hidden="true">
                        ⚠
                    </span>
                    <span className="update-banner-text">
                        {t('workspace.partialLoad', {
                            count: corruptFiles.length,
                            files: corruptFiles.join(', '),
                            defaultValue:
                                '{{count}} item(s) could not be read (invalid JSON): {{files}}. The rest of your workspace loaded normally.',
                        })}
                    </span>
                    <button
                        type="button"
                        className="update-banner-dismiss"
                        aria-label={t('common.close', 'Close')}
                        title={t('common.close', 'Close')}
                        onClick={() => setCorruptFiles([])}
                    >
                        ×
                    </button>
                </div>
            ) : null}

            {/* Every screen that owns the app before the workspace does. The tour cannot
                work these out by looking at the DOM - it tried, and started on top of the
                Home launcher and then the account dialog - so it is told. */}
            <GuidedTour
                ready={
                    !showEngineSetup &&
                    !showProfileSetup &&
                    !showWorkspacePicker &&
                    !showSetupWizard &&
                    !showHome &&
                    !!workspacePathState
                }
            />

            <HomeLauncher
                open={showHome}
                onClose={() => setShowHome(false)}
                workspaceName={workspacePathState?.split(/[\\/]/).filter(Boolean).pop() ?? null}
                showOnStartup={showHomeOnStartup}
                onToggleShowOnStartup={handleToggleShowHomeOnStartup}
                actions={{
                    activePipelineId: activeJobId,
                    // Closing Home is what reveals the canvas underneath.
                    openDesigner: () => {},
                    openDataQuality: () => {},
                    importJob: isInTauri() ? handleImportJob : undefined,
                    openDuckie: () => setShowChatPanel(true),
                    openMcp: () => setShowMcpModal(true),
                    openRuns: () =>
                        window.dispatchEvent(
                            new CustomEvent('duckle:open-tab', { detail: 'history' }),
                        ),
                    openSchedules: () => handleSchedulePipeline(activeJobId),
                    openPlans: () => setShowPlans(true),
                    openBuild: () => handleBuildPipeline(activeJobId),
                    openLineage: () => setShowLineage(true),
                    openTrust: () => setShowTrust(true),
                    openCatalog: () => setShowCatalog(true),
                    openConnections: () => handleNewConnection('connections'),
                    openGit: () => setShowGitPanel(true),
                    openSettings: () => setShowSettings(true),
                }}
            />

            <main className="workspace">
                <LeftSidebar
                    repoItems={repo}
                    activeJobId={activeJobId}
                    openJobIds={openJobIds}
                    onOpenPipeline={handleOpenPipeline}
                    onOpenItem={handleOpenRepoItem}
                    onNewPipeline={openNewPipelineModal}
                    onNewFolder={handleNewFolderInRepo}
                    onNewConnection={handleNewConnection}
                    onImportJob={isInTauri() ? handleImportJob : undefined}
                    onNewContext={handleNewContext}
                    onNewDocument={handleNewDocument}
                    onNewRoutine={handleNewRoutine}
                    onNewDive={handleNewDive}
                    onNewDashboard={handleNewDashboard}
                    onRenameRepoItem={handleRenameRepoItem}
                    onDuplicateRepoItem={handleDuplicateRepoItem}
                    onDeleteRepoItem={requestDeleteRepoItem}
                    onMoveRepoItem={handleMoveRepoItem}
                    onSchedulePipeline={handleSchedulePipeline}
                    onBackfillPipeline={handleBackfillPipeline}
                    onBuildPipeline={handleBuildPipeline}
                    onDeployPipeline={isInTauri() ? setDeployModalPipelineId : undefined}
                />
                <section className="canvas-shell" data-tour="canvas">
                    <EditorHeader
                        jobs={jobs}
                        activeJobId={activeJobId}
                        isRunning={isRunning}
                        onSelectJob={setActiveJobId}
                        onCloseJob={handleCloseJob}
                        onNewJob={handleNewJob}
                        onRun={handleRun}
                        onStop={handleStop}
                        liveMode={liveMode}
                        onToggleLive={handleToggleLive}
                        onSave={handleSave}
                        onValidate={handleValidate}
                        onAutoLayout={handleAutoLayout}
                        onCopySql={handleCopySql}
                        onExportJson={handleExportJson}
                        onDeploy={
                            isInTauri() ? () => setDeployModalPipelineId(activeJobId) : undefined
                        }
                        onExportSqlFile={handleExportSql}
                        onImportJson={handleImportJson}
                        onImportJob={isInTauri() ? handleImportJob : undefined}
                        onGoHome={() => setShowHome(true)}
                    />
                    <EditorTabs
                        engine={engine}
                        nodes={nodes}
                        planNodes={planNodes}
                        edges={edges}
                        runResult={runResult}
                        isRunning={isRunning}
                        nodeLabels={nodeLabels}
                        workspacePath={workspacePathState}
                        pipelineId={activeJobId}
                        onNodesChange={handleNodesChange}
                        onEdgesChange={handleEdgesChange}
                        onConnectWithType={handleConnectWithType}
                        onSelectionChange={handleSelectionChange}
                        onDropComponent={handleDropComponent}
                        onSetActiveContext={setActiveContextId}
                        onNodeAction={handleNodeAction}
                        onPaneAction={handlePaneAction}
                        onEdgeChangeType={handleEdgeChangeType}
                        onEdgeDelete={handleEdgeDelete}
                        onEdgeEdit={handleEdgeEdit}
                        nodeAutodetectAvailable={nodeAutodetectAvailable}
                    />
                </section>
                <PropertiesPanel
                    selected={selectedNode}
                    allNodes={nodes}
                    edges={edges}
                    repoItems={repo}
                    activeContextId={activeContextId}
                    workspacePath={workspacePathState}
                    onUpdate={handleUpdateNode}
                    onOpenMapper={handleOpenMapper}
                    focusNameRequest={renameRequest}
                />
            </main>

            <BottomPanel
                runResult={runResult}
                isRunning={isRunning}
                nodeLabels={nodeLabels}
                terminalNodeIds={terminalNodeIds}
                validation={validation}
                openProblemsRequest={validateRequest}
            />

            <StatusBar
                engine={engine}
                runtime={runtime}
                nodeCount={nodes.length}
                edgeCount={edges.length}
                errorCount={validation.errorCount}
                warningCount={validation.warningCount}
                pipelineName={activeJobName}
            />

            <NewPipelineModal
                open={newPipelineModal.open}
                defaultParentId={newPipelineModal.defaultParent}
                repoItems={repo}
                onCancel={() =>
                    setNewPipelineModal({ open: false, defaultParent: 'pipelines' })
                }
                onCreate={handleCreatePipeline}
            />

            <ImportReportModal report={importReport} onClose={() => setImportReport(null)} />

            <ConfirmModal
                open={pendingDelete !== null}
                title={pendingDelete ? `Delete ${pendingDelete.type}` : 'Delete'}
                message={pendingDelete ? describeDelete(pendingDelete) : ''}
                confirmLabel="Delete"
                onCancel={() => setPendingDelete(null)}
                onConfirm={() => {
                    if (pendingDelete) handleDeleteRepoItem(pendingDelete.id);
                    setPendingDelete(null);
                }}
            />

            {showEngineSetup ? (
                <EngineSetupModal onReady={() => setEngineGate('ready')} />
            ) : null}

            {seeding ? (
                <div className="modal-backdrop modal-backdrop-blocking">
                    <div className="modal modal-engine">
                        <div className="modal-body modal-engine-body">
                            <div className="engine-loading">
                                <Loader2 size={16} className="spin" /> Initialising workspace -
                                adding sample pipelines and generating their data…
                            </div>
                        </div>
                    </div>
                </div>
            ) : null}

            {showProfileSetup ? (
                <ProfileSetupModal onCreate={handleCreateFirstAccount} />
            ) : null}

            {showChatPanel ? (
                <ChatPanel
                    onClose={() => setShowChatPanel(false)}
                    onInsertPipeline={handleInsertAiPipeline}
                />
            ) : null}

            {showGitPanel && workspacePathState ? (
                <GitPanel
                    workspacePath={workspacePathState}
                    onClose={() => setShowGitPanel(false)}
                />
            ) : null}

            {showWorkspacePicker ? (
                <WorkspacePickerModal onPicked={handlePickedWorkspace} />
            ) : null}

            {showSetupWizard ? (
                <SetupWizard
                    workspacePath={workspacePathState ?? ''}
                    onDone={choice => {
                        try {
                            localStorage.setItem('duckle.setupChoice', choice);
                        } catch {
                            /* a browser with storage turned off still gets past the wizard */
                        }
                        setSetupChoice(choice);
                    }}
                />
            ) : null}

            {editingEdge ? (
                <EdgeEditorModal
                    edge={editingEdge}
                    onSave={handleEdgeEditSave}
                    onCancel={() => setEditingEdgeId(null)}
                />
            ) : null}

            {scheduleModalPipelineId ? (
                <ScheduleEditorModal
                    pipelineId={scheduleModalPipelineId}
                    pipelineName={
                        repo.find(r => r.id === scheduleModalPipelineId)?.name ??
                        scheduleModalPipelineId
                    }
                    workspacePath={workspacePathState}
                    onClose={() => setScheduleModalPipelineId(null)}
                />
            ) : null}

            {backfillModalPipelineId ? (
                <BackfillModal
                    pipelineName={
                        repo.find(r => r.id === backfillModalPipelineId)?.name ??
                        backfillModalPipelineId
                    }
                    workspacePath={workspacePathState}
                    onClose={() => setBackfillModalPipelineId(null)}
                />
            ) : null}

            {runParamPrompt ? (
                <RunParametersModal
                    paramNames={runParamPrompt.names}
                    pipelineName={repo.find(r => r.id === activeJobId)?.name ?? activeJobId}
                    onCancel={() => setRunParamPrompt(null)}
                    onSubmit={values => {
                        const target = runParamPrompt.target;
                        setRunParamPrompt(null);
                        void launchRun(target, values);
                    }}
                />
            ) : null}

            {deployModalPipelineId ? (
                <DeployModal
                    pipelineId={deployModalPipelineId}
                    pipelineName={
                        repo.find(r => r.id === deployModalPipelineId)?.name ??
                        deployModalPipelineId
                    }
                    // What the editor holds right now, which is what was just tested here.
                    pipeline={pipelineData[deployModalPipelineId] ?? EMPTY_PIPELINE}
                    workspacePath={workspacePathState}
                    onClose={() => setDeployModalPipelineId(null)}
                />
            ) : null}

            {buildModalPipelineId ? (
                <BuildPipelineModal
                    pipelineId={buildModalPipelineId}
                    pipelineName={
                        repo.find(r => r.id === buildModalPipelineId)?.name ??
                        buildModalPipelineId
                    }
                    workspacePath={workspacePathState}
                    contexts={contexts}
                    onClose={() => setBuildModalPipelineId(null)}
                />
            ) : null}

            {showDivesGallery ? (
                <DivesGallery
                    dives={diveItems}
                    dashboards={dashboardItems}
                    onOpenDive={(id) => {
                        setShowDivesGallery(false);
                        setRepoEditor({ kind: 'dive', itemId: id, parentId: 'dives' });
                    }}
                    onOpenDashboard={(id) => {
                        setShowDivesGallery(false);
                        setRepoEditor({ kind: 'dashboard', itemId: id, parentId: 'dashboards' });
                    }}
                    onNewDive={() => {
                        setShowDivesGallery(false);
                        handleNewDive('dives');
                    }}
                    onNewDashboard={() => {
                        setShowDivesGallery(false);
                        handleNewDashboard('dashboards');
                    }}
                    onClose={() => setShowDivesGallery(false)}
                />
            ) : null}
            {showLineage ? (
                <LineageModal nodes={nodes} edges={edges} onClose={() => setShowLineage(false)} />
            ) : null}
            {showTrust ? (
                <TrustModal
                    nodes={nodes}
                    edges={edges}
                    workspacePath={workspacePathState}
                    onClose={() => setShowTrust(false)}
                />
            ) : null}
            {showCatalog ? (
                <CatalogPanel workspace={workspacePathState} onClose={() => setShowCatalog(false)} />
            ) : null}
            {showPlans ? (
                <PlansModal
                    workspacePath={workspacePathState}
                    pipelines={repo
                        .filter(r => r.type === 'pipeline')
                        .map(r => ({ id: r.id, name: r.name }))}
                    onClose={() => setShowPlans(false)}
                />
            ) : null}
            {showMcpModal ? <McpModal onClose={() => setShowMcpModal(false)} /> : null}
            {showSettings ? (
                <SettingsModal workspace={workspacePathState} onClose={() => setShowSettings(false)} />
            ) : null}

            {repoEditor?.kind === 'connection' ? (
                <ConnectionEditorModal
                    item={editingRepoItem}
                    onSave={handleSaveConnection}
                    onCancel={() => setRepoEditor(null)}
                />
            ) : null}
            {repoEditor?.kind === 'context' ? (
                <ContextEditorModal
                    item={editingRepoItem}
                    onSave={handleSaveContext}
                    onCancel={() => setRepoEditor(null)}
                />
            ) : null}
            {repoEditor?.kind === 'document' ? (
                <DocumentEditorModal
                    item={editingRepoItem}
                    onSave={handleSaveDocument}
                    onCancel={() => setRepoEditor(null)}
                />
            ) : null}
            {repoEditor?.kind === 'routine' ? (
                <RoutineEditorModal
                    item={editingRepoItem}
                    onSave={handleSaveRoutine}
                    onCancel={() => setRepoEditor(null)}
                />
            ) : null}
            {repoEditor?.kind === 'dive' ? (
                <DiveModal
                    item={editingRepoItem}
                    workspacePath={workspacePathState}
                    theme={theme === 'light' ? 'light' : 'dark'}
                    onSave={handleSaveDive}
                    onClose={() => setRepoEditor(null)}
                />
            ) : null}
            {repoEditor?.kind === 'dashboard' ? (
                <DashboardModal
                    item={editingRepoItem}
                    diveItems={diveItems}
                    workspacePath={workspacePathState}
                    theme={theme === 'light' ? 'light' : 'dark'}
                    onSave={handleSaveDashboard}
                    onClose={() => setRepoEditor(null)}
                />
            ) : null}

            {mapperNode ? (
                <VisualMapperModal
                    nodeId={mapperNode.id}
                    nodeLabel={mapperNode.data.label}
                    nodes={nodes}
                    edges={edges}
                    initialState={
                        {
                            // outputs first so a saved mapper missing it (e.g. an
                            // AI- or hand-authored node) still yields a valid array
                            // and never crashes the modal (#93).
                            outputs: [] as MappingRow[],
                            ...((mapperNode.data.properties?.mapper as
                                | MapperState
                                | undefined) ?? {}),
                            lookups:
                                (mapperNode.data.properties?.lookups as
                                    | LookupConfig[]
                                    | undefined) ?? [],
                        } as MapperState
                    }
                    onSave={handleMapperSave}
                    onCancel={() => setMapperNodeId(null)}
                />
            ) : null}
        </div>
        </RunStatusContext.Provider>
    );
}
