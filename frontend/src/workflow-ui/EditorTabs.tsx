import { useCallback, useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import type {
    Connection,
    Edge,
    EdgeChange,
    Node,
    NodeChange,
    OnSelectionChangeParams,
} from '@xyflow/react';
import Canvas, { type DropPosition, type NodeAction, type PaneAction } from '../canvas/Canvas';
import PlanView from './PlanView';
import RunView from './RunView';
import RunHistoryView from './RunHistoryView';
import TestsView from './TestsView';
import type { EngineId } from './EngineSelector';
import type { DuckleNodeData, NoteData } from '../pipeline-types';
import type { ComponentDef } from './palette-data';
import type { ConnectionType } from '../canvas/connection-types';
import type { RunResult } from '../tauri-bridge';

type TabId = 'canvas' | 'plan' | 'run' | 'history' | 'tests';

// Tab labels resolved per-render via useTranslation; we keep just the IDs here.
const TAB_IDS: TabId[] = ['canvas', 'plan', 'run', 'history', 'tests'];

// Newer tabs are not yet in every locale file, so each has a readable fallback.
const TAB_FALLBACK: Partial<Record<TabId, string>> = { history: 'History', tests: 'Tests' };

type Props = {
    engine: EngineId;
    nodes: Node<DuckleNodeData>[];
    /** Nodes with ${...} placeholders resolved, for the Plan preview (#105). */
    planNodes: Node<DuckleNodeData>[];
    notes: Node<NoteData>[];
    edges: Edge[];
    runResult: RunResult | null;
    isRunning: boolean;
    nodeLabels: Record<string, string>;
    workspacePath: string | null;
    pipelineId: string | null;
    onNodesChange: (changes: NodeChange[]) => void;
    onNotesChange: (changes: NodeChange[]) => void;
    onEdgesChange: (changes: EdgeChange[]) => void;
    onConnectWithType: (connection: Connection, type: ConnectionType) => void;
    onSelectionChange: (params: OnSelectionChangeParams) => void;
    onDropComponent: (component: ComponentDef, position: DropPosition) => void;
    onSetActiveContext?: (id: string) => void;
    onNodeAction: (action: NodeAction, nodeId: string) => void;
    onPaneAction: (action: PaneAction) => void;
    onEdgeChangeType: (edgeId: string, newType: ConnectionType) => void;
    onEdgeDelete: (edgeId: string) => void;
    onEdgeEdit: (edgeId: string) => void;
    nodeAutodetectAvailable: (nodeId: string) => boolean;
    onSaveBeforeTests: () => Promise<void>;
};

export default function EditorTabs({
    engine: _engine,
    nodes,
    edges,
    runResult,
    isRunning,
    planNodes,
    notes,
    nodeLabels,
    workspacePath,
    pipelineId,
    onNodesChange,
    onNotesChange,
    onEdgesChange,
    onConnectWithType,
    onSelectionChange,
    onDropComponent,
    onSetActiveContext,
    onNodeAction,
    onPaneAction,
    onEdgeChangeType,
    onEdgeDelete,
    onEdgeEdit,
    nodeAutodetectAvailable,
    onSaveBeforeTests,
}: Props) {
    const { t } = useTranslation();
    const [active, setActive] = useState<TabId>('canvas');
    const [testNode, setTestNode] = useState<string | null>(null);
    const clearTestNode = useCallback(() => setTestNode(null), []);

    // Let anything outside this subtree select a tab, the same way the tour and
    // the Dives toggle are driven (a window event rather than lifted state).
    // The Home launcher uses it to open Runs without owning the tab state.
    useEffect(() => {
        const onOpenTab = (e: Event) => {
            const id = (e as CustomEvent<string>).detail;
            if ((TAB_IDS as string[]).includes(id)) setActive(id as TabId);
        };
        window.addEventListener('duckle:open-tab', onOpenTab);
        return () => window.removeEventListener('duckle:open-tab', onOpenTab);
    }, []);

    // A node's Create test: open Tests with a new case on that node.
    useEffect(() => {
        const onCreateTest = (e: Event) => {
            setTestNode((e as CustomEvent<string>).detail);
            setActive('tests');
        };
        window.addEventListener('duckle:create-test', onCreateTest);
        return () => window.removeEventListener('duckle:create-test', onCreateTest);
    }, []);

    return (
        <div className="editor">
            <div className="tabbar" data-tour="tabs" role="tablist" aria-label={t('editorTabs.ariaLabel')}>
                {TAB_IDS.map(id => (
                    <button
                        key={id}
                        type="button"
                        role="tab"
                        aria-selected={active === id}
                        className="tab"
                        onClick={() => setActive(id)}
                    >
                        {t(`editorTabs.${id}`, { defaultValue: TAB_FALLBACK[id] ?? id })}
                    </button>
                ))}
            </div>
            <div className="tab-content">
                <div className={'tab-panel' + (active === 'canvas' ? ' tab-panel-active' : '')}>
                    <Canvas
                        nodes={nodes}
                        notes={notes}
                        edges={edges}
                        pipelineId={pipelineId}
                        onNodesChange={onNodesChange}
                        onNotesChange={onNotesChange}
                        onEdgesChange={onEdgesChange}
                        onConnectWithType={onConnectWithType}
                        onSelectionChange={onSelectionChange}
                        onDropComponent={onDropComponent}
                        onSetActiveContext={onSetActiveContext}
                        onNodeAction={onNodeAction}
                        onPaneAction={onPaneAction}
                        onEdgeChangeType={onEdgeChangeType}
                        onEdgeDelete={onEdgeDelete}
                        onEdgeEdit={onEdgeEdit}
                        nodeAutodetectAvailable={nodeAutodetectAvailable}
                    />
                </div>
                <div className={'tab-panel' + (active === 'plan' ? ' tab-panel-active' : '')}>
                    <PlanView nodes={planNodes} edges={edges} workspacePath={workspacePath} />
                </div>
                <div className={'tab-panel' + (active === 'run' ? ' tab-panel-active' : '')}>
                    <RunView
                        runResult={runResult}
                        isRunning={isRunning}
                        nodeLabels={nodeLabels}
                    />
                </div>
                <div className={'tab-panel' + (active === 'history' ? ' tab-panel-active' : '')}>
                    {/* Mount lazily so history only loads when the tab is opened;
                        runResult identity changes after each run, triggering a reload. */}
                    {active === 'history' ? (
                        <RunHistoryView
                            workspacePath={workspacePath}
                            pipelineId={pipelineId}
                            runResultKey={runResult?.duration_ms ?? 0}
                        />
                    ) : null}
                </div>
                <div className={'tab-panel' + (active === 'tests' ? ' tab-panel-active' : '')}>
                    {active === 'tests' ? (
                        <TestsView
                            workspacePath={workspacePath}
                            pipelineId={pipelineId}
                            nodes={nodes}
                            resolvedNodes={planNodes}
                            edges={edges}
                            onSaveBeforeRun={onSaveBeforeTests}
                            newForNode={testNode}
                            onNewForNodeTaken={clearTestNode}
                        />
                    ) : null}
                </div>
            </div>
        </div>
    );
}
