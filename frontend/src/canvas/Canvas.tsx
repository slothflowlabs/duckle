import { useCallback, useEffect, useRef, useState } from 'react';
import {
    ReactFlow,
    ReactFlowProvider,
    Background,
    Controls,
    MiniMap,
    useReactFlow,
    type Connection,
    type Edge,
    type EdgeChange,
    type Node,
    type NodeChange,
    type OnSelectionChangeParams,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import {
    Check,
    ClipboardCopy,
    ClipboardPaste,
    Copy,
    Hash,
    LayoutGrid,
    Maximize2,
    MousePointer2,
    Package,
    Pencil,
    Play,
    Power,
    Redo2,
    SlidersHorizontal,
    Sparkles,
    Trash2,
    Undo2,
    FlaskConical,
    Workflow,
} from 'lucide-react';
import DuckleNode from './nodes/DuckleNode';
import DuckleEdge from './DuckleEdge';
import ConnectionTypePicker from './ConnectionTypePicker';
import { QuickAddSearch } from './QuickAddSearch';
import { CONNECTION_TYPES, type ConnectionType } from './connection-types';
import { loadPersisted } from '../persistence';
import type { DuckleNodeData } from '../pipeline-types';
import { readClipboard } from '../clipboard';
import type { ComponentDef } from '../workflow-ui/palette-data';
import { useContextMenu, type MenuItem } from '../workflow-ui/ContextMenu';
import { getManifest } from '../workflow-ui/fields/component-manifests';
import { useTheme } from '../theme';

const ICON_SIZE = 14;

const nodeTypes = {
    source: DuckleNode,
    transform: DuckleNode,
    sink: DuckleNode,
};

const edgeTypes = {
    duckle: DuckleEdge,
};

const DEFAULT_EDGE_OPTIONS = {
    type: 'duckle' as const,
};

const DELETE_KEYS = ['Delete', 'Backspace'];

const PRO_OPTIONS = { hideAttribution: true };

export type DropPosition = { x: number; y: number };

/** Whether a key press is meant for the canvas: focus on it or on nothing, never in a text field. */
function canvasHasKeyboard(wrapper: HTMLElement | null): boolean {
    // Another editor tab hides the canvas without unmounting it, and a quick-add
    // opened there sat invisible until the canvas came back.
    if (!wrapper || getComputedStyle(wrapper).visibility !== 'visible') return false;
    const ae = document.activeElement as HTMLElement | null;
    const inField =
        !!ae &&
        (ae.tagName === 'INPUT' ||
            ae.tagName === 'TEXTAREA' ||
            ae.tagName === 'SELECT' ||
            ae.isContentEditable);
    const inCanvas = ae === document.body || (wrapper?.contains(ae) ?? false);
    return !inField && inCanvas;
}

export type NodeAction =
    | 'open'
    | 'rename'
    | 'duplicate'
    | 'copy'
    | 'toggle-disable'
    | 'autodetect'
    | 'run-from-here'
    | 'create-test'
    | 'copy-id'
    | 'delete';

export type PaneAction =
    | 'paste'
    | 'select-all'
    | 'auto-layout'
    | 'undo'
    | 'redo'
    | 'build';

type Props = {
    nodes: Node<DuckleNodeData>[];
    edges: Edge[];
    onNodesChange: (changes: NodeChange[]) => void;
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
    /** Active pipeline id - used to remember pan/zoom per pipeline. */
    pipelineId?: string | null;
};

function CanvasInner({
    nodes,
    edges,
    onNodesChange,
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
    pipelineId,
}: Props) {
    const { screenToFlowPosition, setViewport, fitView } = useReactFlow();
    const { theme } = useTheme();

    // Remember pan/zoom per pipeline (per user = localStorage on this machine),
    // so reopening a pipeline restores where you left off instead of snapping to
    // a default fit. Restore-in-progress is tracked so the programmatic
    // setViewport doesn't immediately overwrite the saved value via onMoveEnd.
    const viewportRestoredFor = useRef<string | null>(null);
    const viewportKey = (id: string) => `duckle.viewport.${id}`;
    useEffect(() => {
        const id = pipelineId;
        if (!id) return;
        viewportRestoredFor.current = null;
        // rAF so React Flow has the new pipeline's nodes before we fit/restore.
        const raf = requestAnimationFrame(() => {
            const saved = localStorage.getItem(viewportKey(id));
            if (saved) {
                try {
                    const vp = JSON.parse(saved);
                    if (
                        typeof vp?.x === 'number' &&
                        typeof vp?.y === 'number' &&
                        typeof vp?.zoom === 'number'
                    ) {
                        setViewport(vp);
                    } else {
                        fitView();
                    }
                } catch {
                    fitView();
                }
            } else {
                fitView();
            }
            viewportRestoredFor.current = id;
        });
        return () => cancelAnimationFrame(raf);
    }, [pipelineId, setViewport, fitView]);

    const handleMoveEnd = useCallback(
        (_: unknown, vp: { x: number; y: number; zoom: number }) => {
            const id = pipelineId;
            // Skip saves triggered by the restore itself (before it completes).
            if (!id || viewportRestoredFor.current !== id) return;
            try {
                localStorage.setItem(
                    viewportKey(id),
                    JSON.stringify({ x: vp.x, y: vp.y, zoom: vp.zoom }),
                );
            } catch {
                /* localStorage full / unavailable - non-fatal */
            }
        },
        [pipelineId],
    );
    const menu = useContextMenu();
    const mouseRef = useRef({ x: 0, y: 0 });
    // The minimap eats a corner of the canvas, which on a dense graph is the
    // part you are trying to see. Settings toggles it and broadcasts, so an
    // open canvas reacts without a reload.
    const [showMinimap, setShowMinimap] = useState(() => !loadPersisted('hideMinimap', false));
    useEffect(() => {
        const sync = () => setShowMinimap(!loadPersisted('hideMinimap', false));
        window.addEventListener('duckle:minimap-visibility', sync);
        return () => window.removeEventListener('duckle:minimap-visibility', sync);
    }, []);
    const [pendingConnection, setPendingConnection] = useState<Connection | null>(null);
    const [pickerPos, setPickerPos] = useState<{ x: number; y: number } | null>(null);
    const [pickerAllowed, setPickerAllowed] = useState<Set<ConnectionType> | null>(null);
    const wrapperRef = useRef<HTMLDivElement>(null);
    // Quick-add: null = closed; a string = open, seeded with the first typed char.
    const [quickAddSeed, setQuickAddSeed] = useState<string | null>(null);

    const onMouseMove = useCallback((e: React.MouseEvent) => {
        mouseRef.current = { x: e.clientX, y: e.clientY };
    }, []);

    // Start typing on the canvas to open the quick-add component search. Only
    // fires for a bare printable key while focus is on the canvas/body (never
    // inside a text field, the properties panel, or a modal) so normal typing
    // and shortcuts are untouched.
    useEffect(() => {
        const onKey = (e: KeyboardEvent) => {
            if (quickAddSeed !== null) return;
            if (e.metaKey || e.ctrlKey || e.altKey) return;
            if (e.key.length !== 1 || e.key === ' ') return;
            if (!canvasHasKeyboard(wrapperRef.current)) return;
            e.preventDefault();
            setQuickAddSeed(e.key);
        };
        document.addEventListener('keydown', onKey);
        return () => document.removeEventListener('keydown', onKey);
    }, [quickAddSeed]);

    // A click on the bare canvas leaves the focus on the page body, and so does a
    // click on the run log, so the focus alone cannot say a key is for the canvas.
    // Where the last click landed can.
    const clickedCanvasLast = useRef(false);
    useEffect(() => {
        const onDown = (e: PointerEvent) => {
            clickedCanvasLast.current = wrapperRef.current?.contains(e.target as HTMLElement | null) ?? false;
        };
        document.addEventListener('pointerdown', onDown, true);
        return () => document.removeEventListener('pointerdown', onDown, true);
    }, []);

    // The shortcuts the context menus show beside their items. Only while the
    // canvas has the keyboard, so a text field or the run log keeps its own
    // Ctrl+A, and the browser keeps Ctrl+D and Ctrl+0 everywhere else.
    useEffect(() => {
        const onKey = (e: KeyboardEvent) => {
            if (!canvasHasKeyboard(wrapperRef.current)) return;
            if (document.activeElement === document.body && !clickedCanvasLast.current) return;
            const ctrl = e.ctrlKey || e.metaKey;
            const key = e.key.toLowerCase();
            const selected = nodes.filter(n => n.selected);
            const only = selected.length === 1 ? selected[0].id : null;
            if (e.key === 'F2' && !ctrl) {
                if (only) onNodeAction('rename', only);
            } else if (ctrl && key === 'd') {
                if (only) onNodeAction('duplicate', only);
            } else if (ctrl && key === 'a') {
                onPaneAction('select-all');
            } else if (ctrl && key === '0') {
                fitView();
            } else {
                return;
            }
            e.preventDefault();
        };
        document.addEventListener('keydown', onKey);
        return () => document.removeEventListener('keydown', onKey);
    }, [nodes, onNodeAction, onPaneAction, fitView]);

    const handleQuickAdd = useCallback(
        (component: ComponentDef) => {
            // Drop where the cursor last was; fall back to the pane centre if
            // the mouse hasn't moved yet (e.g. right after loading).
            let screen = mouseRef.current;
            if (!screen.x && !screen.y) {
                const r = wrapperRef.current?.getBoundingClientRect();
                if (r) screen = { x: r.left + r.width / 2, y: r.top + r.height / 2 };
            }
            onDropComponent(component, screenToFlowPosition(screen));
            setQuickAddSeed(null);
        },
        [onDropComponent, screenToFlowPosition],
    );

    const handleConnectStart = useCallback(() => {
        // Capture position at start; in case onConnect doesn't fire (cancelled drag)
        // mouse ref keeps tracking.
    }, []);

    const handleConnect = useCallback(
        (connection: Connection) => {
            const sourceNode = nodes.find(n => n.id === connection.source);
            const targetNode = nodes.find(n => n.id === connection.target);
            const sourceManifest = sourceNode
                ? getManifest(sourceNode.data.componentId)
                : undefined;
            const targetManifest = targetNode
                ? getManifest(targetNode.data.componentId)
                : undefined;
            const sourcePort = sourceManifest?.ports?.outputs.find(
                p => p.id === connection.sourceHandle,
            );
            const targetPort = targetManifest?.ports?.inputs.find(
                p => p.id === connection.targetHandle,
            );

            // If the user dropped on a specifically-typed input port
            // (lookup, iterate, reject), honor that - the connection
            // type matches the port.
            if (targetPort && targetPort.type !== 'main') {
                onConnectWithType(connection, targetPort.type);
                return;
            }

            // If the source port emits a specific row type, that wins
            // (a reject output emits a reject row).
            const portType = sourcePort?.type;
            if (portType && portType !== 'main') {
                onConnectWithType(connection, portType);
                return;
            }

            // Compute which connection types are available given the
            // target's accepted input ports. Lookup is gated to
            // components that declare a lookup input.
            const acceptedInputTypes = new Set(
                (targetManifest?.ports?.inputs ?? []).map(p => p.type),
            );
            const allowed = new Set<ConnectionType>();
            allowed.add('main');
            if (acceptedInputTypes.has('lookup')) allowed.add('lookup');
            if (acceptedInputTypes.has('iterate')) allowed.add('iterate');
            if (acceptedInputTypes.has('filter')) allowed.add('filter');
            if (acceptedInputTypes.has('reject')) allowed.add('reject');
            // Triggers are always available - they target a component as
            // a whole, not a specific input port.
            allowed.add('on-subjob-ok');
            allowed.add('on-subjob-error');
            allowed.add('on-component-ok');
            allowed.add('on-component-error');
            allowed.add('if');
            allowed.add('run-if');

            setPendingConnection(connection);
            setPickerAllowed(allowed);
            setPickerPos({ x: mouseRef.current.x, y: mouseRef.current.y });
        },
        [nodes, onConnectWithType],
    );

    const handlePickType = useCallback(
        (type: ConnectionType) => {
            if (pendingConnection) {
                onConnectWithType(pendingConnection, type);
            }
            setPendingConnection(null);
            setPickerPos(null);
            setPickerAllowed(null);
        },
        [pendingConnection, onConnectWithType],
    );

    const handleCancelPick = useCallback(() => {
        setPendingConnection(null);
        setPickerPos(null);
        setPickerAllowed(null);
    }, []);

    const handleDragOver = useCallback((e: React.DragEvent) => {
        if (
            e.dataTransfer.types.includes('application/duckle-component') ||
            e.dataTransfer.types.includes('application/duckle-context')
        ) {
            e.preventDefault();
            e.dataTransfer.dropEffect = 'copy';
        }
    }, []);

    const handleDrop = useCallback(
        (e: React.DragEvent) => {
            e.preventDefault();
            e.stopPropagation();
            // A context dragged from the Project tree sets the active context.
            const ctxId = e.dataTransfer.getData('application/duckle-context');
            if (ctxId) {
                onSetActiveContext?.(ctxId);
                return;
            }
            const raw = e.dataTransfer.getData('application/duckle-component');
            if (!raw) {
                // Helpful when debugging: types should include our MIME.
                console.warn(
                    'Drop received but application/duckle-component data is missing',
                    Array.from(e.dataTransfer.types),
                );
                return;
            }
            try {
                const component = JSON.parse(raw) as ComponentDef;
                const position = screenToFlowPosition({ x: e.clientX, y: e.clientY });
                onDropComponent(component, position);
            } catch (err) {
                console.error('Failed to parse dropped component', err);
            }
        },
        [onDropComponent, onSetActiveContext, screenToFlowPosition],
    );

    const handleNodeContextMenu = useCallback(
        (e: React.MouseEvent, node: Node<DuckleNodeData>) => {
            const isDisabled = node.data.disabled === true;
            const autodetect = nodeAutodetectAvailable(node.id);
            const items: MenuItem[] = [
                {
                    kind: 'header',
                    key: 'header',
                    label: node.data.label + '  #' + node.id.slice(0, 6),
                },
                node.data.componentId === 'xf.map'
                    ? {
                          kind: 'item',
                          key: 'open',
                          label: 'Open visual mapper',
                          icon: <Workflow size={ICON_SIZE} />,
                          shortcut: 'Dbl-click',
                          onClick: () => onNodeAction('open', node.id),
                      }
                    : {
                          kind: 'item',
                          key: 'open',
                          label: 'Properties',
                          icon: <SlidersHorizontal size={ICON_SIZE} />,
                          shortcut: 'Dbl-click',
                          onClick: () => onNodeAction('open', node.id),
                      },
                {
                    kind: 'item',
                    key: 'rename',
                    label: 'Rename',
                    icon: <Pencil size={ICON_SIZE} />,
                    shortcut: 'F2',
                    onClick: () => onNodeAction('rename', node.id),
                },
                {
                    kind: 'item',
                    key: 'duplicate',
                    label: 'Duplicate',
                    icon: <Copy size={ICON_SIZE} />,
                    shortcut: 'Ctrl+D',
                    onClick: () => onNodeAction('duplicate', node.id),
                },
                {
                    kind: 'item',
                    key: 'copy',
                    label: 'Copy',
                    icon: <ClipboardCopy size={ICON_SIZE} />,
                    shortcut: 'Ctrl+C',
                    onClick: () => onNodeAction('copy', node.id),
                },
                {
                    kind: 'item',
                    key: 'toggle-disable',
                    label: isDisabled ? 'Enable' : 'Disable',
                    icon: <Power size={ICON_SIZE} />,
                    onClick: () => onNodeAction('toggle-disable', node.id),
                },
                { kind: 'separator', key: 's1' },
                {
                    kind: 'item',
                    key: 'run',
                    label: 'Run to here (this node + all upstream)',
                    icon: <Play size={ICON_SIZE} />,
                    onClick: () => onNodeAction('run-from-here', node.id),
                    disabled: isDisabled,
                },
                {
                    kind: 'item',
                    key: 'create-test',
                    label: 'Create test',
                    icon: <FlaskConical size={ICON_SIZE} />,
                    onClick: () => onNodeAction('create-test', node.id),
                },
                {
                    kind: 'item',
                    key: 'autodetect',
                    label: 'Auto-detect schema',
                    icon: <Sparkles size={ICON_SIZE} />,
                    onClick: () => onNodeAction('autodetect', node.id),
                    disabled: !autodetect,
                },
                { kind: 'separator', key: 's2' },
                {
                    kind: 'item',
                    key: 'copy-id',
                    label: 'Copy ID',
                    icon: <Hash size={ICON_SIZE} />,
                    onClick: () => onNodeAction('copy-id', node.id),
                },
                {
                    kind: 'item',
                    key: 'delete',
                    label: 'Delete',
                    icon: <Trash2 size={ICON_SIZE} />,
                    shortcut: 'Del',
                    onClick: () => onNodeAction('delete', node.id),
                    danger: true,
                },
            ];
            menu.open(e, items);
        },
        [menu, onNodeAction, nodeAutodetectAvailable],
    );

    const handleEdgeContextMenu = useCallback(
        (e: React.MouseEvent, edge: Edge) => {
            const currentType = (edge.data as { connectionType?: ConnectionType } | undefined)
                ?.connectionType ?? 'main';
            const items: MenuItem[] = [
                {
                    kind: 'header',
                    key: 'header',
                    label: 'Connection · ' + edge.id.slice(0, 8),
                },
                {
                    kind: 'item',
                    key: 'edit',
                    label: 'Edit label / condition…',
                    icon: <Pencil size={ICON_SIZE} />,
                    shortcut: 'Dbl-click',
                    onClick: () => onEdgeEdit(edge.id),
                },
                { kind: 'separator', key: 's0' },
                ...CONNECTION_TYPES.map((t): MenuItem => ({
                    kind: 'item',
                    key: 'type-' + t.id,
                    label: t.label,
                    icon: currentType === t.id ? <Check size={ICON_SIZE} /> : null,
                    onClick: () => onEdgeChangeType(edge.id, t.id),
                })),
                { kind: 'separator', key: 's1' },
                {
                    kind: 'item',
                    key: 'delete',
                    label: 'Delete connection',
                    icon: <Trash2 size={ICON_SIZE} />,
                    shortcut: 'Del',
                    onClick: () => onEdgeDelete(edge.id),
                    danger: true,
                },
            ];
            menu.open(e, items);
        },
        [menu, onEdgeChangeType, onEdgeDelete, onEdgeEdit],
    );

    const handlePaneContextMenu = useCallback(
        (e: React.MouseEvent | MouseEvent) => {
            const items: MenuItem[] = [
                { kind: 'header', key: 'header', label: 'Canvas' },
                {
                    kind: 'item',
                    key: 'undo',
                    label: 'Undo',
                    icon: <Undo2 size={ICON_SIZE} />,
                    shortcut: 'Ctrl+Z',
                    onClick: () => onPaneAction('undo'),
                },
                {
                    kind: 'item',
                    key: 'redo',
                    label: 'Redo',
                    icon: <Redo2 size={ICON_SIZE} />,
                    shortcut: 'Ctrl+Y',
                    onClick: () => onPaneAction('redo'),
                },
                { kind: 'separator', key: 's0' },
                {
                    kind: 'item',
                    key: 'fit',
                    label: 'Fit to view',
                    icon: <Maximize2 size={ICON_SIZE} />,
                    shortcut: 'Ctrl+0',
                    onClick: () => fitView(),
                },
                {
                    kind: 'item',
                    key: 'layout',
                    label: 'Auto-layout',
                    icon: <LayoutGrid size={ICON_SIZE} />,
                    onClick: () => onPaneAction('auto-layout'),
                },
                { kind: 'separator', key: 's1' },
                {
                    kind: 'item',
                    key: 'select-all',
                    label: 'Select all',
                    icon: <MousePointer2 size={ICON_SIZE} />,
                    shortcut: 'Ctrl+A',
                    onClick: () => onPaneAction('select-all'),
                },
                {
                    kind: 'item',
                    key: 'paste',
                    label: 'Paste',
                    icon: <ClipboardPaste size={ICON_SIZE} />,
                    shortcut: 'Ctrl+V',
                    onClick: () => onPaneAction('paste'),
                    disabled: readClipboard() === null,
                },
                { kind: 'separator', key: 's2' },
                {
                    kind: 'item',
                    key: 'build',
                    label: 'Build pipeline…',
                    icon: <Package size={ICON_SIZE} />,
                    onClick: () => onPaneAction('build'),
                },
            ];
            menu.open(e, items);
        },
        [menu, onPaneAction, fitView],
    );

    return (
        <div
            ref={wrapperRef}
            className="canvas-dnd"
            onDragOver={handleDragOver}
            onDrop={handleDrop}
            onMouseMove={onMouseMove}
        >
            <ReactFlow
                nodes={nodes}
                edges={edges}
                onNodesChange={onNodesChange}
                onEdgesChange={onEdgesChange}
                onConnect={handleConnect}
                onConnectStart={handleConnectStart}
                onSelectionChange={onSelectionChange}
                onNodeContextMenu={handleNodeContextMenu}
                onEdgeContextMenu={handleEdgeContextMenu}
                onNodeDoubleClick={(_, node) => onNodeAction('open', node.id)}
                onEdgeDoubleClick={(_, edge) => onEdgeEdit(edge.id)}
                onPaneContextMenu={handlePaneContextMenu}
                nodeTypes={nodeTypes}
                edgeTypes={edgeTypes}
                defaultEdgeOptions={DEFAULT_EDGE_OPTIONS}
                deleteKeyCode={DELETE_KEYS}
                proOptions={PRO_OPTIONS}
                onMoveEnd={handleMoveEnd}
                colorMode={theme}
            >
                <Background gap={16} />
                {showMinimap ? <MiniMap pannable zoomable /> : null}
                <Controls />
            </ReactFlow>
            {menu.element}
            {pickerPos ? (
                <ConnectionTypePicker
                    position={pickerPos}
                    allowedTypes={pickerAllowed ?? undefined}
                    onPick={handlePickType}
                    onCancel={handleCancelPick}
                />
            ) : null}
            {quickAddSeed !== null ? (
                <QuickAddSearch
                    initialQuery={quickAddSeed}
                    onPick={handleQuickAdd}
                    onClose={() => setQuickAddSeed(null)}
                />
            ) : null}
        </div>
    );
}

export default function Canvas(props: Props) {
    return (
        <ReactFlowProvider>
            <CanvasInner {...props} />
        </ReactFlowProvider>
    );
}
