// Canvas groups: named, coloured frames around steps that move and collapse
// together, as Talend, Hop and NiFi have them. A group is plain data kept beside
// a pipeline's nodes - its label, colour, whether it is collapsed, and which
// steps it holds - so nothing that runs or compiles a pipeline ever sees one.
// The frame itself is never stored: it is drawn around wherever its steps are.

import type { Edge, Node } from '@xyflow/react';

export type GroupColor = 'orange' | 'blue' | 'slate' | 'purple' | 'yellow' | 'red';
export const GROUP_COLORS: GroupColor[] = ['orange', 'blue', 'slate', 'purple', 'yellow', 'red'];

export type PipelineGroup = {
    id: string;
    label: string;
    color?: GroupColor;
    collapsed?: boolean;
    /** Ids of the steps in the group. A step is in one group at most. */
    members: string[];
};

export type Rect = { x: number; y: number; width: number; height: number };

/** Space between a frame's edge and the steps inside it. */
export const FRAME_PAD = 24;
/** The header strip above the steps, which carries the label and is the drag handle. */
export const FRAME_HEAD = 36;
/** A collapsed group is drawn at this size, at the top left of where its steps are. */
export const COLLAPSED_SIZE = { width: 240, height: 64 };
/** Size assumed for a step the canvas has not measured yet. */
export const UNMEASURED_SIZE = { width: 300, height: 160 };
/** Edges drawn to a collapsed frame in place of one of its steps carry ids with this prefix. */
export const PROXY_EDGE = 'group-edge:';

function sizeOf(n: Node): { width: number; height: number } {
    return {
        width: n.measured?.width ?? n.width ?? UNMEASURED_SIZE.width,
        height: n.measured?.height ?? n.height ?? UNMEASURED_SIZE.height,
    };
}

/**
 * The box a group's frame is drawn in, from where its steps are now: around
 * them with room for the header, or a card at the same corner when collapsed,
 * so collapsing and expanding never moves the group. Null when none of its
 * steps is on the canvas.
 */
export function frameRect(group: PipelineGroup, nodes: Node[]): Rect | null {
    const members = new Set(group.members);
    let minX = Infinity;
    let minY = Infinity;
    let maxX = -Infinity;
    let maxY = -Infinity;
    for (const n of nodes) {
        if (!members.has(n.id)) continue;
        const { width, height } = sizeOf(n);
        minX = Math.min(minX, n.position.x);
        minY = Math.min(minY, n.position.y);
        maxX = Math.max(maxX, n.position.x + width);
        maxY = Math.max(maxY, n.position.y + height);
    }
    if (minX === Infinity) return null;
    const x = minX - FRAME_PAD;
    const y = minY - FRAME_PAD - FRAME_HEAD;
    if (group.collapsed) return { x, y, ...COLLAPSED_SIZE };
    return { x, y, width: maxX - minX + 2 * FRAME_PAD, height: maxY - minY + 2 * FRAME_PAD + FRAME_HEAD };
}

/**
 * Where a group's steps go when its frame is dragged to `to`: each moves by the
 * same amount as the frame. A step in `alsoMoving` is already being moved by
 * the same drag (it was selected with the frame) and is left to that move.
 */
export function memberMoves(
    group: PipelineGroup,
    nodes: Node[],
    to: { x: number; y: number },
    alsoMoving: Set<string>,
): { id: string; position: { x: number; y: number } }[] {
    const frame = frameRect(group, nodes);
    if (!frame) return [];
    const dx = to.x - frame.x;
    const dy = to.y - frame.y;
    if (dx === 0 && dy === 0) return [];
    const members = new Set(group.members);
    return nodes
        .filter(n => members.has(n.id) && !alsoMoving.has(n.id))
        .map(n => ({ id: n.id, position: { x: n.position.x + dx, y: n.position.y + dy } }));
}

/** The groups without `ids` in them, dropping any group that leaves empty. */
function without(groups: PipelineGroup[], ids: Set<string>): PipelineGroup[] {
    const out: PipelineGroup[] = [];
    for (const g of groups) {
        const members = g.members.filter(m => !ids.has(m));
        if (members.length === 0) continue;
        out.push(members.length === g.members.length ? g : { ...g, members });
    }
    return out;
}

/**
 * Put `ids` in a new group. A step is in one group at most, so they leave any
 * group they were in, and a group that leaves empty goes. The new group takes
 * the first colour no other group has.
 */
export function groupNodes(groups: PipelineGroup[], ids: string[], label: string, id: string): PipelineGroup[] {
    const members = [...new Set(ids)];
    if (members.length === 0) return groups;
    const rest = without(groups, new Set(members));
    const used = new Set(rest.map(g => g.color ?? 'orange'));
    const color = GROUP_COLORS.find(c => !used.has(c)) ?? GROUP_COLORS[rest.length % GROUP_COLORS.length];
    return [...rest, { id, label, color, members }];
}

/** Take one step out of whatever group holds it. */
export function ungroupNode(groups: PipelineGroup[], nodeId: string): PipelineGroup[] {
    return without(groups, new Set([nodeId]));
}

/**
 * The groups with steps that no longer exist taken out. The same array comes
 * back when nothing changed, so a caller can tell a real change from none.
 */
export function pruneGroups(groups: PipelineGroup[], nodeIds: Set<string>): PipelineGroup[] {
    const gone = new Set<string>();
    for (const g of groups) for (const m of g.members) if (!nodeIds.has(m)) gone.add(m);
    return gone.size === 0 ? groups : without(groups, gone);
}

/** Each step hidden inside a collapsed group, with the group that hides it. */
export function hiddenSteps(groups: PipelineGroup[]): Map<string, string> {
    const out = new Map<string, string>();
    for (const g of groups) if (g.collapsed) for (const m of g.members) out.set(m, g.id);
    return out;
}

/**
 * The links to draw. A link inside a collapsed group is not drawn; one that
 * crosses into or out of it is drawn to the group's frame instead. Those are
 * pictures of the real link, so they cannot be selected or deleted: an action
 * on the real link waits until the group is expanded. The same array comes back
 * when no group is collapsed.
 */
export function edgesForDisplay(edges: Edge[], groups: PipelineGroup[]): Edge[] {
    const hidden = hiddenSteps(groups);
    if (hidden.size === 0) return edges;
    const out: Edge[] = [];
    for (const e of edges) {
        const from = hidden.get(e.source);
        const to = hidden.get(e.target);
        if (!from && !to) {
            out.push(e);
            continue;
        }
        if (from && from === to) continue;
        out.push({
            ...e,
            id: PROXY_EDGE + e.id,
            source: from ?? e.source,
            target: to ?? e.target,
            sourceHandle: from ? 'group-out' : e.sourceHandle,
            targetHandle: to ? 'group-in' : e.targetHandle,
            selected: false,
            selectable: false,
            deletable: false,
            focusable: false,
        });
    }
    return out;
}
