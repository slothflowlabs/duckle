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
import { Braces, FolderOpen, GitBranch, Moon, Sparkles, Sun } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import LanguageSelector from './i18n/LanguageSelector';
import { UpdateBanner } from './UpdateBanner';
import EditorTabs from './workflow-ui/EditorTabs';
import EditorHeader, { type Job } from './workflow-ui/EditorHeader';
import EngineSelector, { type EngineId } from './workflow-ui/EngineSelector';
import { useTheme } from './theme';
import { loadPersisted, savePersisted } from './persistence';
import { resolveOutputSchema } from './schema-resolve';
import {
    cancelPipeline,
    compilePipelineSql,
    runPipeline,
    runPipelinePartial,
    scheduleSetWorkspace,
    type PipelineEvent,
    type RunResult,
} from './tauri-bridge';
import ScheduleEditorModal from './workflow-ui/ScheduleEditorModal';
import EngineSetupModal from './workflow-ui/EngineSetupModal';
import ChatPanel from './workflow-ui/ChatPanel';
import GitPanel from './workflow-ui/GitPanel';
import CiStatusBadge from './workflow-ui/CiStatusBadge';
import WindowControls from './workflow-ui/WindowControls';
import { engineStatus } from './tauri-bridge';
import { copyText, saveTextFile } from './tauri-io';
import { RunStatusContext } from './canvas/run-status-context';
import { validatePipeline } from './validation';
import { resolveForRun } from './run-resolve';
import WorkspacePickerModal from './workflow-ui/WorkspacePickerModal';
import {
    deleteItemPayload,
    deletePipelineFile,
    getWorkspacePath,
    isInTauri,
    loadWorkspace,
    saveItemPayload,
    saveMetadata,
    savePipelineFile,
    saveRepository,
    setWorkspacePath,
} from './workspace';
import LeftSidebar from './workflow-ui/LeftSidebar';
import PropertiesPanel from './workflow-ui/PropertiesPanel';
import BottomPanel from './workflow-ui/BottomPanel';
import StatusBar from './workflow-ui/StatusBar';
import NewPipelineModal, { type PipelineTemplate } from './workflow-ui/NewPipelineModal';
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
import type { RepoItem } from './repo-types';

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
            properties: { hasHeader: true },
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
            properties: {},
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
    return { nodes: [], edges: [] };
}

const EMPTY_PIPELINE: PipelineState = { nodes: [], edges: [] };

export default function App() {
    const { t } = useTranslation();
    const { theme, toggle: toggleTheme } = useTheme();
    const [runtime, setRuntime] = useState<RuntimeState>('connecting');
    const [engine, setEngine] = useState<EngineId>(() =>
        loadPersisted<EngineId>('engine', 'duckdb'),
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
            setEngineGate(duck?.installed ? 'ready' : 'engine-setup');
        });
        return () => {
            cancelled = true;
        };
    }, []);

    const [workspacePathState, setWorkspacePathState] = useState<string | null>(() =>
        getWorkspacePath(),
    );
    // In Tauri: needs workspace picked + hydrated before saves start.
    // In browser: workspaceReady is always true; localStorage persists.
    const [workspaceReady, setWorkspaceReady] = useState<boolean>(!isInTauri());
    // Engine setup comes first; only once engines are ready do we show
    // the workspace picker.
    const showEngineSetup = isInTauri() && engineGate === 'engine-setup';
    const showWorkspacePicker =
        isInTauri() && engineGate === 'ready' && !workspacePathState;
    const [newPipelineModal, setNewPipelineModal] = useState<{
        open: boolean;
        defaultParent: string;
    }>({ open: false, defaultParent: 'pipelines' });

    const activePipeline = pipelineData[activeJobId] ?? EMPTY_PIPELINE;
    const nodes = activePipeline.nodes;
    const edges = activePipeline.edges;

    // Hydrate from workspace file on Tauri once the path is known.
    useEffect(() => {
        if (!isInTauri() || !workspacePathState) return;
        let cancelled = false;
        loadWorkspace(workspacePathState).then(state => {
            if (cancelled) return;
            if (state) {
                if (state.engine) setEngine(state.engine as EngineId);
                if (state.pipelineData)
                    setPipelineData(state.pipelineData as Record<string, PipelineState>);
                if (state.repo) setRepo(state.repo as RepoItem[]);
                if (state.jobs) setJobs(state.jobs as Job[]);
                if (state.activeJobId) setActiveJobId(state.activeJobId);
            }
            setWorkspaceReady(true);
        });
        return () => {
            cancelled = true;
        };
    }, [workspacePathState]);

    // Granular Tauri saves - each slice goes to its own file so the
    // workspace folder is git-friendly. Browser still uses localStorage.
    const prevPipelineDataRef = useRef<Record<string, PipelineState> | null>(null);
    const prevRepoRef = useRef<RepoItem[] | null>(null);

    useEffect(() => {
        if (!workspaceReady || !isInTauri() || !workspacePathState) return;
        const ws = workspacePathState;
        const t = setTimeout(() => {
            void saveMetadata(ws, { engine, jobs, activeJobId });
        }, 200);
        return () => clearTimeout(t);
    }, [workspaceReady, workspacePathState, engine, jobs, activeJobId]);

    useEffect(() => {
        if (!workspaceReady || !isInTauri() || !workspacePathState) return;
        const ws = workspacePathState;
        const t = setTimeout(() => {
            void (async () => {
                await saveRepository(ws, repo as unknown as Array<Record<string, unknown>>);
                // Diff payload-bearing items against the previous snapshot
                // so we only write the ones that actually changed.
                const prev = prevRepoRef.current ?? [];
                const prevById = new Map(prev.map(i => [i.id, i]));
                const currById = new Map(repo.map(i => [i.id, i]));
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
                            await saveItemPayload(ws, item.type, item.id, item.payload);
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
                prevRepoRef.current = repo;
            })();
        }, 300);
        return () => clearTimeout(t);
    }, [workspaceReady, workspacePathState, repo]);

    useEffect(() => {
        if (!workspaceReady || !isInTauri() || !workspacePathState) return;
        const ws = workspacePathState;
        const t = setTimeout(() => {
            void (async () => {
                const prev = prevPipelineDataRef.current ?? {};
                for (const [id, state] of Object.entries(pipelineData)) {
                    if (prev[id] !== state) {
                        await savePipelineFile(ws, id, state);
                    }
                }
                prevPipelineDataRef.current = pipelineData;
            })();
        }, 400);
        return () => clearTimeout(t);
    }, [workspaceReady, workspacePathState, pipelineData]);

    // Browser fallback: localStorage (unchanged).
    useEffect(() => {
        if (!workspaceReady) return;
        if (isInTauri() && workspacePathState) return;
        const t = setTimeout(() => {
            savePersisted('pipelines', pipelineData);
            savePersisted('repo', repo);
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
        setWorkspacePath(picked);
        setWorkspacePathState(picked);
    }, [workspacePathState]);

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
        setSelectedId(params.nodes[0]?.id ?? null);
    }, []);

    const handleUpdateNode = useCallback(
        (id: string, patch: Partial<DuckleNodeData>) => {
            setNodes(ns =>
                ns.map(n => (n.id === id ? { ...n, data: { ...n.data, ...patch } } : n)),
            );
            markDirty();
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
                        return { status: 'ok', duration_ms: 0, nodes: {}, preview: [] };
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
                setRunResult(result);
                // Merge the previews back into each node's data so the
                // Preview tab and the inline schema badge stay in sync
                // with what just ran.
                if (result.preview.length > 0) {
                    const byId = new Map(result.preview.map(p => [p.node_id, p]));
                    setNodes(ns =>
                        ns.map(n => {
                            const p = byId.get(n.id);
                            if (!p) return n;
                            return {
                                ...n,
                                data: {
                                    ...n.data,
                                    schema: p.columns,
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
        () => validatePipeline(nodes, edges),
        [nodes, edges],
    );

    const [validateRequest, setValidateRequest] = useState<number>(0);
    const handleValidate = useCallback(() => {
        // Just bump a counter so BottomPanel pops the Problems tab.
        setValidateRequest(n => n + 1);
    }, []);

    const handleRun = useCallback(() => {
        // Don't launch a run that's guaranteed to fail (e.g. a sink with
        // no output path) - that only yields a cryptic engine error.
        // Surface the Problems tab so the user can fix it first.
        if (validation.errorCount > 0) {
            setValidateRequest(n => n + 1);
            return;
        }
        setIsRunning(true);
        setRunResult(null);
        const start = performance.now();
        // Inline SQL routines + substitute ${context.var} before running;
        // the canvas keeps the editable, un-substituted values.
        const runNodes = resolveForRun(nodes, repo);
        void runPipeline(runNodes, edges, handleEvent, activeJobId, workspacePathState)
            .then(result => finishRun(start, result))
            .finally(() => setIsRunning(false));
    }, [nodes, edges, repo, handleEvent, finishRun, activeJobId, workspacePathState, validation.errorCount]);

    const handleRunFromHere = useCallback(
        (nodeId: string) => {
            if (validation.errorCount > 0) {
                setValidateRequest(n => n + 1);
                return;
            }
            setIsRunning(true);
            setRunResult(null);
            const start = performance.now();
            const runNodes = resolveForRun(nodes, repo);
            void runPipelinePartial(
                runNodes,
                edges,
                nodeId,
                handleEvent,
                activeJobId,
                workspacePathState,
            )
                .then(result => finishRun(start, result))
                .finally(() => setIsRunning(false));
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
        setJobs(js => js.map(j => (j.id === activeJobId ? { ...j, dirty: false } : j)));
    }, [activeJobId]);

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

    const handleAutoLayout = useCallback(() => {
        setNodes(ns =>
            ns.map((n, i) => ({
                ...n,
                position: { x: 60 + i * 280, y: 140 },
            })),
        );
    }, [setNodes]);

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
                void manifest.autodetect(newNode.data.properties ?? {}).then(result => {
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
                    void manifest.autodetect(node.data.properties ?? {}).then(result => {
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
        [nodes, selectedId, setNodes, setEdges, markDirty, handleRunFromHere],
    );

    const handlePaneAction = useCallback(
        (action: PaneAction) => {
            switch (action) {
                case 'auto-layout':
                    handleAutoLayout();
                    break;
                case 'select-all':
                    setNodes(ns => ns.map(n => ({ ...n, selected: true })));
                    break;
                case 'paste':
                    break;
            }
        },
        [handleAutoLayout, setNodes],
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
            const id = 'f_' + Date.now().toString(36);
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

    const handleDuplicateRepoItem = useCallback(
        (id: string) => {
            const item = repo.find(i => i.id === id);
            if (!item) return;
            const newId = item.type[0] + '_' + Date.now().toString(36);
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
                nodes?: Array<{ id?: string; type?: string; data?: { label?: string; props?: unknown } }>;
                edges?: Array<{ id?: string; source?: string; target?: string }>;
            };
            if (!Array.isArray(obj.nodes)) return;
            const nodes: Node<DuckleNodeData>[] = obj.nodes.map((n, i) => ({
                id: n.id ?? `n${i + 1}`,
                type: nodeKindFromComponent(n.type ?? 'src.csv'),
                position: { x: 80 + i * 260, y: 160 },
                data: {
                    label: n.data?.label ?? (n.type ?? 'Node').replace(/^[^.]+\./, ''),
                    componentId: n.type ?? 'src.csv',
                    props: (n.data?.props as Record<string, unknown> | undefined) ?? {},
                } as DuckleNodeData,
            }));
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
    }, []);

    const editingRepoItem = useMemo(
        () => (repoEditor?.itemId ? repo.find(i => i.id === repoEditor.itemId) ?? null : null),
        [repoEditor, repo],
    );

    const upsertRepoItem = useCallback(
        (
            type: 'connection' | 'context' | 'doc' | 'routine',
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

    const openJobIds = useMemo(() => new Set(jobs.map(j => j.id)), [jobs]);

    // Note: double-click-to-maximize on the title bar is handled natively
    // by Tauri's `data-tauri-drag-region`. A custom onDoubleClick handler
    // here would call toggleMaximize() a SECOND time, so the window
    // maximized then immediately restored. Do not re-add one.

    return (
        <RunStatusContext.Provider value={runResult?.nodes ?? {}}>
        <div className="app">
            <header
                className="topbar"
                data-tauri-drag-region
            >
                <div className="brand" data-tauri-drag-region>
                    <span className="brand-mark" aria-hidden="true">
                        D
                    </span>
                    Duckle
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
                {contexts.length > 0 ? (
                    <div
                        className="topbar-context"
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
                {workspacePathState ? <CiStatusBadge workspacePath={workspacePathState} /> : null}
                <button
                    type="button"
                    className="topbar-theme-toggle"
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
                    onClick={() => setShowChatPanel(s => !s)}
                    title={t('topbar.duckieAssistant')}
                    aria-label={t('topbar.duckieAssistantAriaToggle')}
                    aria-pressed={showChatPanel}
                >
                    <Sparkles size={14} />
                </button>
                <LanguageSelector />
                <button
                    type="button"
                    className="topbar-theme-toggle"
                    onClick={toggleTheme}
                    title={theme === 'dark' ? t('topbar.switchLight') : t('topbar.switchDark')}
                    aria-label={t('topbar.themeAriaToggle')}
                >
                    {theme === 'dark' ? <Sun size={14} /> : <Moon size={14} />}
                </button>
                <div className="status" data-state={runtime}>
                    <span className="status-dot" /> {t('topbar.runtime', { name: runtime })}
                </div>
                <WindowControls />
            </header>

            <UpdateBanner />

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
                    onNewContext={handleNewContext}
                    onNewDocument={handleNewDocument}
                    onNewRoutine={handleNewRoutine}
                    onRenameRepoItem={handleRenameRepoItem}
                    onDuplicateRepoItem={handleDuplicateRepoItem}
                    onDeleteRepoItem={handleDeleteRepoItem}
                    onSchedulePipeline={handleSchedulePipeline}
                />
                <section className="canvas-shell">
                    <EditorHeader
                        jobs={jobs}
                        activeJobId={activeJobId}
                        isRunning={isRunning}
                        onSelectJob={setActiveJobId}
                        onCloseJob={handleCloseJob}
                        onNewJob={handleNewJob}
                        onRun={handleRun}
                        onStop={handleStop}
                        onSave={handleSave}
                        onValidate={handleValidate}
                        onAutoLayout={handleAutoLayout}
                        onCopySql={handleCopySql}
                        onExportJson={handleExportJson}
                        onExportSqlFile={handleExportSql}
                        onImportJson={handleImportJson}
                    />
                    <EditorTabs
                        engine={engine}
                        nodes={nodes}
                        edges={edges}
                        runResult={runResult}
                        isRunning={isRunning}
                        nodeLabels={nodeLabels}
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

            {showEngineSetup ? (
                <EngineSetupModal onReady={() => setEngineGate('ready')} />
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

            {mapperNode ? (
                <VisualMapperModal
                    nodeId={mapperNode.id}
                    nodeLabel={mapperNode.data.label}
                    nodes={nodes}
                    edges={edges}
                    initialState={
                        {
                            ...((mapperNode.data.properties?.mapper as
                                | MapperState
                                | undefined) ?? { outputs: [] as MappingRow[] }),
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
