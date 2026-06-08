import { useCallback, useRef, useState } from 'react';
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
    Sparkles,
    Trash2,
    Undo2,
} from 'lucide-react';
import DuckleNode from './nodes/DuckleNode';
import DuckleEdge from './DuckleEdge';
import ConnectionTypePicker from './ConnectionTypePicker';
import { CONNECTION_TYPES, type ConnectionType } from './connection-types';
import type { DuckleNodeData } from '../pipeline-types';
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

export type NodeAction =
    | 'rename'
    | 'duplicate'
    | 'toggle-disable'
    | 'autodetect'
    | 'run-from-here'
    | 'copy-id'
    | 'delete';

export type PaneAction =
    | 'paste'
    | 'select-all'
    | 'auto-layout'
    | 'fit-view'
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
}: Props) {
    const { screenToFlowPosition } = useReactFlow();
    const { theme } = useTheme();
    const menu = useContextMenu();
    const mouseRef = useRef({ x: 0, y: 0 });
    const [pendingConnection, setPendingConnection] = useState<Connection | null>(null);
    const [pickerPos, setPickerPos] = useState<{ x: number; y: number } | null>(null);
    const [pickerAllowed, setPickerAllowed] = useState<Set<ConnectionType> | null>(null);

    const onMouseMove = useCallback((e: React.MouseEvent) => {
        mouseRef.current = { x: e.clientX, y: e.clientY };
    }, []);

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
                    key: 'toggle-disable',
                    label: isDisabled ? 'Enable' : 'Disable',
                    icon: <Power size={ICON_SIZE} />,
                    onClick: () => onNodeAction('toggle-disable', node.id),
                },
                { kind: 'separator', key: 's1' },
                {
                    kind: 'item',
                    key: 'run',
                    label: 'Run from here',
                    icon: <Play size={ICON_SIZE} />,
                    onClick: () => onNodeAction('run-from-here', node.id),
                    disabled: isDisabled,
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
                    onClick: () => onPaneAction('fit-view'),
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
                    disabled: true,
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
        [menu, onPaneAction],
    );

    return (
        <div
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
                onEdgeDoubleClick={(_, edge) => onEdgeEdit(edge.id)}
                onPaneContextMenu={handlePaneContextMenu}
                nodeTypes={nodeTypes}
                edgeTypes={edgeTypes}
                defaultEdgeOptions={DEFAULT_EDGE_OPTIONS}
                deleteKeyCode={DELETE_KEYS}
                proOptions={PRO_OPTIONS}
                fitView
                colorMode={theme}
            >
                <Background gap={16} />
                <MiniMap pannable zoomable />
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
