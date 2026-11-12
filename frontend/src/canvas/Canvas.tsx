import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
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
    type NodeSelectionChange,
    type OnSelectionChangeParams,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import {
    AlignEndHorizontal,
    AlignEndVertical,
    AlignHorizontalSpaceAround,
    AlignStartHorizontal,
    AlignStartVertical,
    AlignVerticalSpaceAround,
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
    StickyNote,
    Trash2,
    Undo2,
    FlaskConical,
    Workflow,
    FoldVertical,
    CircleDot,
    CircleOff,
    Group,
    Palette,
    UnfoldVertical,
    Ungroup,
} from 'lucide-react';
import DuckleNode from './nodes/DuckleNode';
import NoteNode from './nodes/NoteNode';
import { NoteContext } from './note-context';
import GroupFrame, { type FrameData } from './nodes/GroupFrame';
import { FrameContext } from './frame-context';
import { PipelineGraphContext } from './graph-context';
import {
    GROUP_COLORS,
    PROXY_EDGE,
    edgesForDisplay,
    frameRect,
    groupNodes,
    hiddenSteps,
    memberMoves,
    ungroupNode,
    type PipelineGroup,
} from './groups';
import { arrange, type ArrangeMode } from './arrange';
import DuckleEdge from './DuckleEdge';
import ConnectionTypePicker from './ConnectionTypePicker';
import { QuickAddSearch } from './QuickAddSearch';
import { CONNECTION_TYPES, type ConnectionType } from './connection-types';
import { loadPersisted, savePersisted } from '../persistence';
import type { DuckleNodeData, NoteData } from '../pipeline-types';
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
    note: NoteNode,
    frame: GroupFrame,
};

const edgeTypes = {
    duckle: DuckleEdge,
};

const DEFAULT_EDGE_OPTIONS = {
    type: 'duckle' as const,
};

const DELETE_KEYS = ['Delete', 'Backspace'];

const PRO_OPTIONS = { hideAttribution: true };

// The same step as the background's dots, so a snapped node sits on them.
const SNAP_GRID: [number, number] = [16, 16];

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
    /** Notes on the canvas: drawn with the nodes, never among them. */
    notes: Node<NoteData>[];
    /** Groups on the canvas: frames drawn around steps, never among the nodes either. */
    groups: PipelineGroup[];
    onGroupsChange: (update: (groups: PipelineGroup[]) => PipelineGroup[]) => void;
    edges: Edge[];
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
    /** Open the breakpoint of a link for editing, or take it off. */
    onEdgeBreakpoint: (edgeId: string, remove?: boolean) => void;
    nodeAutodetectAvailable: (nodeId: string) => boolean;
    /** Active pipeline id - used to remember pan/zoom per pipeline. */
    pipelineId?: string | null;
};

function CanvasInner({
    nodes,
    notes,
    groups,
    onGroupsChange,
    edges,
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
    onEdgeBreakpoint,
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

    // A group's frame is drawn from where its steps are and never stored, and the
    // steps of a collapsed group are hidden for drawing only.
    const [selectedFrames, setSelectedFrames] = useState<ReadonlySet<string>>(() => new Set());
    const frameIds = useMemo(() => new Set(groups.map(g => g.id)), [groups]);
    const frames = useMemo<Node<FrameData>[]>(
        () =>
            groups.flatMap(group => {
                const box = frameRect(group, nodes);
                if (!box) return [];
                const steps = nodes.filter(n => group.members.includes(n.id)).length;
                return [
                    {
                        id: group.id,
                        type: 'frame',
                        position: { x: box.x, y: box.y },
                        width: box.width,
                        height: box.height,
                        style: { width: box.width, height: box.height },
                        data: { group, steps },
                        // Open, it sits behind its steps and only its header takes
                        // the pointer; collapsed, the card stands in for them.
                        zIndex: group.collapsed ? 0 : -1,
                        className: group.collapsed ? 'frame-closed' : 'frame-open',
                        dragHandle: group.collapsed ? undefined : '.canvas-frame-head',
                        connectable: false,
                        selected: selectedFrames.has(group.id),
                    },
                ];
            }),
        [groups, nodes, selectedFrames],
    );
    const hidden = useMemo(() => hiddenSteps(groups), [groups]);
    const shownNodes = useMemo(
        () => (hidden.size === 0 ? nodes : nodes.map(n => (hidden.has(n.id) ? { ...n, hidden: true } : n))),
        [nodes, hidden],
    );
    const flowEdges = useMemo(() => edgesForDisplay(edges, groups), [edges, groups]);
    const graph = useMemo(() => ({ nodes, edges }), [nodes, edges]);

    // Notes are drawn with the nodes but kept apart from them: every change to a
    // note goes back to the notes, so nothing that reads the nodes sees one.
    const noteIds = useMemo(() => new Set(notes.map(n => n.id)), [notes]);
    const flowNodes = useMemo<Node[]>(() => [...frames, ...notes, ...shownNodes], [frames, notes, shownNodes]);
    const handleFlowNodesChange = useCallback(
        (changes: NodeChange[]) => {
            const isNote = (c: NodeChange) => c.type !== 'add' && noteIds.has(c.id);
            const isFrame = (c: NodeChange) => c.type !== 'add' && frameIds.has(c.id);
            const toNotes = changes.filter(isNote);
            const toFrames = changes.filter(isFrame);
            const toNodes = changes.filter(c => !isNote(c) && !isFrame(c));
            if (toFrames.length) {
                // Dragging a frame moves its steps; a step selected with the frame
                // is already moving with the same drag.
                const moving = new Set(toNodes.flatMap(c => (c.type === 'position' ? [c.id] : [])));
                for (const c of toFrames) {
                    if (c.type !== 'position' || !c.position) continue;
                    const group = groups.find(g => g.id === c.id);
                    if (!group) continue;
                    for (const m of memberMoves(group, nodes, c.position, moving)) {
                        toNodes.push({ type: 'position', id: m.id, position: m.position, dragging: c.dragging });
                    }
                }
                const selects = toFrames.filter((c): c is NodeSelectionChange => c.type === 'select');
                if (selects.length) {
                    setSelectedFrames(prev => {
                        const next = new Set(prev);
                        for (const c of selects) {
                            if (c.selected) next.add(c.id);
                            else next.delete(c.id);
                        }
                        return next;
                    });
                }
                // Deleting a frame lets go of its steps; it never deletes them.
                const removed = new Set(toFrames.flatMap(c => (c.type === 'remove' ? [c.id] : [])));
                if (removed.size) onGroupsChange(gs => gs.filter(g => !removed.has(g.id)));
            }
            if (toNotes.length) onNotesChange(toNotes);
            if (toNodes.length) onNodesChange(toNodes);
        },
        [noteIds, frameIds, groups, nodes, onNotesChange, onNodesChange, onGroupsChange],
    );
    // A link drawn to a collapsed frame is a picture of a real one, so nothing
    // done to the picture may reach the real link.
    const handleFlowEdgesChange = useCallback(
        (changes: EdgeChange[]) => {
            const real = changes.filter(c => !('id' in c) || !c.id.startsWith(PROXY_EDGE));
            if (real.length) onEdgesChange(real);
        },
        [onEdgesChange],
    );
    const [editingFrame, setEditingFrame] = useState<string | null>(null);
    const frameActions = useMemo(
        () => ({
            editing: editingFrame,
            setEditing: setEditingFrame,
            rename: (id: string, label: string) =>
                onGroupsChange(gs => gs.map(g => (g.id === id ? { ...g, label } : g))),
            toggle: (id: string) =>
                onGroupsChange(gs => gs.map(g => (g.id === id ? { ...g, collapsed: !g.collapsed } : g))),
        }),
        [editingFrame, onGroupsChange],
    );
    // Grouping acts on the selection when the step right-clicked is part of one,
    // else on that step alone.
    const groupItems = useCallback(
        (nodeId: string): MenuItem[] => {
            const chosen = nodes.filter(n => n.selected);
            const ids = chosen.some(n => n.id === nodeId) ? chosen.map(n => n.id) : [nodeId];
            const holder = groups.find(g => g.members.includes(nodeId));
            const items: MenuItem[] = [
                { kind: 'separator', key: 's-group' },
                {
                    kind: 'item',
                    key: 'group',
                    label: ids.length > 1 ? `Group ${ids.length} steps` : 'Group this step',
                    icon: <Group size={ICON_SIZE} />,
                    onClick: () => {
                        const id = 'group_' + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2, 7);
                        onGroupsChange(gs => groupNodes(gs, ids, `Group ${gs.length + 1}`, id));
                        setEditingFrame(id);
                    },
                },
            ];
            if (holder) {
                items.push({
                    kind: 'item',
                    key: 'ungroup-step',
                    label: `Take out of "${holder.label}"`,
                    icon: <Ungroup size={ICON_SIZE} />,
                    onClick: () => onGroupsChange(gs => ungroupNode(gs, nodeId)),
                });
            }
            return items;
        },
        [nodes, groups, onGroupsChange],
    );
    const [editingNote, setEditingNote] = useState<string | null>(null);
    const noteActions = useMemo(
        () => ({
            editing: editingNote,
            setEditing: setEditingNote,
            commit: (id: string, text: string) => {
                const note = notes.find(n => n.id === id);
                if (note) onNotesChange([{ type: 'replace', id, item: { ...note, data: { ...note.data, text } } }]);
            },
        }),
        [editingNote, notes, onNotesChange],
    );
    const addNote = useCallback(
        (position: { x: number; y: number }) => {
            const id = 'note_' + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2, 7);
            onNotesChange([{ type: 'add', item: { id, type: 'note', position, data: { text: '' } } }]);
            setEditingNote(id);
        },
        [onNotesChange],
    );

    // Snap to grid: off until asked for, so a drag lands where it always did.
    const [snapToGrid, setSnapToGrid] = useState(() => loadPersisted('snapToGrid', false));
    const toggleSnap = useCallback(() => {
        setSnapToGrid(on => {
            savePersisted('snapToGrid', !on);
            return !on;
        });
    }, []);

    // Align and distribute act on the selection, when the node right-clicked is
    // part of one; a single node has nothing to line up with.
    const arrangeItems = useCallback(
        (nodeId: string): MenuItem[] => {
            const chosen = flowNodes.filter(n => n.selected && n.type !== 'frame');
            if (chosen.length < 2 || !chosen.some(n => n.id === nodeId)) return [];
            const item = (mode: ArrangeMode, label: string, icon: ReactNode): MenuItem => ({
                kind: 'item',
                key: 'arrange-' + mode,
                label,
                icon,
                onClick: () => {
                    const moves = arrange(chosen, mode);
                    if (moves.length) {
                        handleFlowNodesChange(moves.map(m => ({ type: 'position', id: m.id, position: m.position })));
                    }
                },
            });
            return [
                { kind: 'separator', key: 's-arrange' },
                item('left', 'Align left', <AlignStartVertical size={ICON_SIZE} />),
                item('right', 'Align right', <AlignEndVertical size={ICON_SIZE} />),
                item('top', 'Align top', <AlignStartHorizontal size={ICON_SIZE} />),
                item('bottom', 'Align bottom', <AlignEndHorizontal size={ICON_SIZE} />),
                ...(chosen.length >= 3
                    ? [
                          item('distribute-x', 'Distribute horizontally', <AlignHorizontalSpaceAround size={ICON_SIZE} />),
                          item('distribute-y', 'Distribute vertically', <AlignVerticalSpaceAround size={ICON_SIZE} />),
                      ]
                    : []),
            ];
        },
        [flowNodes, handleFlowNodesChange],
    );

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
        (e: React.MouseEvent, flowNode: Node) => {
            const frame = groups.find(g => g.id === flowNode.id);
            if (frame) {
                const nextColor = GROUP_COLORS[(GROUP_COLORS.indexOf(frame.color ?? 'orange') + 1) % GROUP_COLORS.length];
                menu.open(e, [
                    { kind: 'header', key: 'header', label: 'Group · ' + frame.label },
                    {
                        kind: 'item',
                        key: 'rename',
                        label: 'Rename',
                        icon: <Pencil size={ICON_SIZE} />,
                        shortcut: 'Dbl-click',
                        onClick: () => setEditingFrame(frame.id),
                    },
                    {
                        kind: 'item',
                        key: 'toggle',
                        label: frame.collapsed ? 'Expand' : 'Collapse',
                        icon: frame.collapsed ? <UnfoldVertical size={ICON_SIZE} /> : <FoldVertical size={ICON_SIZE} />,
                        onClick: () => frameActions.toggle(frame.id),
                    },
                    {
                        kind: 'item',
                        key: 'colour',
                        label: `Change colour (to ${nextColor})`,
                        icon: <Palette size={ICON_SIZE} />,
                        onClick: () =>
                            onGroupsChange(gs => gs.map(g => (g.id === frame.id ? { ...g, color: nextColor } : g))),
                    },
                    { kind: 'separator', key: 's1' },
                    {
                        kind: 'item',
                        key: 'ungroup',
                        label: 'Ungroup (keeps the steps)',
                        icon: <Ungroup size={ICON_SIZE} />,
                        shortcut: 'Del',
                        onClick: () => onGroupsChange(gs => gs.filter(g => g.id !== frame.id)),
                    },
                ]);
                return;
            }
            if (noteIds.has(flowNode.id)) {
                menu.open(e, [
                    { kind: 'header', key: 'header', label: 'Note' },
                    {
                        kind: 'item',
                        key: 'edit',
                        label: 'Edit text',
                        icon: <Pencil size={ICON_SIZE} />,
                        shortcut: 'Dbl-click',
                        onClick: () => setEditingNote(flowNode.id),
                    },
                    {
                        kind: 'item',
                        key: 'delete',
                        label: 'Delete',
                        icon: <Trash2 size={ICON_SIZE} />,
                        shortcut: 'Del',
                        onClick: () => onNotesChange([{ type: 'remove', id: flowNode.id }]),
                        danger: true,
                    },
                ]);
                return;
            }
            const node = flowNode as Node<DuckleNodeData>;
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
                ...arrangeItems(node.id),
                ...groupItems(node.id),
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
        [menu, onNodeAction, nodeAutodetectAvailable, noteIds, onNotesChange, arrangeItems, groups, frameActions, onGroupsChange, groupItems],
    );

    const handleEdgeContextMenu = useCallback(
        (e: React.MouseEvent, edge: Edge) => {
            // A link drawn to a collapsed group has no menu: it is not the link.
            if (edge.id.startsWith(PROXY_EDGE)) return;
            const currentType = (edge.data as { connectionType?: ConnectionType } | undefined)
                ?.connectionType ?? 'main';
            const breakpoint = (edge.data as { breakpoint?: string } | undefined)?.breakpoint;
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
                {
                    kind: 'item',
                    key: 'breakpoint',
                    label: breakpoint ? 'Edit breakpoint…' : 'Set breakpoint…',
                    icon: <CircleDot size={ICON_SIZE} />,
                    onClick: () => onEdgeBreakpoint(edge.id),
                },
                ...(breakpoint
                    ? [
                          {
                              kind: 'item',
                              key: 'breakpoint-off',
                              label: 'Remove breakpoint',
                              icon: <CircleOff size={ICON_SIZE} />,
                              onClick: () => onEdgeBreakpoint(edge.id, true),
                          } as MenuItem,
                      ]
                    : []),
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
        [menu, onEdgeChangeType, onEdgeDelete, onEdgeEdit, onEdgeBreakpoint],
    );

    const handlePaneContextMenu = useCallback(
        (e: React.MouseEvent | MouseEvent) => {
            const at = screenToFlowPosition({ x: e.clientX, y: e.clientY });
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
                {
                    kind: 'item',
                    key: 'snap',
                    label: 'Snap to grid',
                    icon: snapToGrid ? <Check size={ICON_SIZE} /> : null,
                    onClick: toggleSnap,
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
                {
                    kind: 'item',
                    key: 'note',
                    label: 'Add note',
                    icon: <StickyNote size={ICON_SIZE} />,
                    onClick: () => addNote(at),
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
        [menu, onPaneAction, fitView, screenToFlowPosition, snapToGrid, toggleSnap, addNote],
    );

    return (
        <div
            ref={wrapperRef}
            className="canvas-dnd"
            onDragOver={handleDragOver}
            onDrop={handleDrop}
            onMouseMove={onMouseMove}
        >
            <NoteContext.Provider value={noteActions}>
              <FrameContext.Provider value={frameActions}>
              <PipelineGraphContext.Provider value={graph}>
                <ReactFlow
                    nodes={flowNodes}
                    edges={flowEdges}
                    onNodesChange={handleFlowNodesChange}
                    onEdgesChange={handleFlowEdgesChange}
                    onConnect={handleConnect}
                    onConnectStart={handleConnectStart}
                    onSelectionChange={onSelectionChange}
                    onNodeContextMenu={handleNodeContextMenu}
                    onEdgeContextMenu={handleEdgeContextMenu}
                    onNodeDoubleClick={(_, node) => {
                        // A note opens itself for writing (see NoteNode), and a
                        // group renames from its label (see GroupFrame).
                        if (!noteIds.has(node.id) && !frameIds.has(node.id)) onNodeAction('open', node.id);
                    }}
                    onEdgeDoubleClick={(_, edge) => {
                        if (!edge.id.startsWith(PROXY_EDGE)) onEdgeEdit(edge.id);
                    }}
                    onPaneContextMenu={handlePaneContextMenu}
                    nodeTypes={nodeTypes}
                    edgeTypes={edgeTypes}
                    defaultEdgeOptions={DEFAULT_EDGE_OPTIONS}
                    deleteKeyCode={DELETE_KEYS}
                    proOptions={PRO_OPTIONS}
                    onMoveEnd={handleMoveEnd}
                    colorMode={theme}
                    snapToGrid={snapToGrid}
                    snapGrid={SNAP_GRID}
                >
                    <Background gap={16} />
                    {showMinimap ? <MiniMap pannable zoomable /> : null}
                    <Controls />
                </ReactFlow>
              </PipelineGraphContext.Provider>
              </FrameContext.Provider>
            </NoteContext.Provider>
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
