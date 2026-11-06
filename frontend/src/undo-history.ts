// Canvas undo/redo history: a per-pipeline stack of the full {nodes, edges}
// document (covers node add/delete/move, edge changes, and component settings
// edits).
//
// History is captured by observing the active pipeline and recording the
// previous settled snapshot whenever a *meaningful* change lands. "Meaningful"
// excludes pure selection changes and run-preview data (schema / sampleRows),
// so selecting a node or running the pipeline never pollutes history; a burst of
// changes (e.g. a drag) is coalesced into one step via a short debounce.
//
// Kept free of React so it can be driven directly with a fake clock; the hook in
// useUndoRedo.ts only feeds it snapshots and applies what it returns.
import type { Node, Edge } from '@xyflow/react';

export type CanvasSnapshot = { nodes: Node<Record<string, unknown>>[]; edges: Edge[]; notes?: Node<Record<string, unknown>>[] };

/** Run `fn` after `ms`; the returned function cancels it. */
export type Timer = (fn: () => void, ms: number) => () => void;

const HISTORY_LIMIT = 50;
const DEBOUNCE_MS = 350;

/// A stable string of just the parts a user can edit, excluding selection
/// state and run-only data, so noise changes don't create history entries.
function meaningfulKey(s: CanvasSnapshot): string {
    const nodes = (s.nodes ?? []).map(n => {
        const d = (n.data ?? {}) as Record<string, unknown>;
        return {
            id: n.id,
            x: Math.round(n.position?.x ?? 0),
            y: Math.round(n.position?.y ?? 0),
            label: d.label,
            componentId: d.componentId,
            properties: d.properties,
            disabled: d.disabled,
            // The SQL name. Left out, renaming it was not a step, and the next
            // undo reverted it together with whatever came before.
            alias: d.alias,
        };
    });
    const edges = (s.edges ?? []).map(e => ({
        id: e.id,
        source: e.source,
        target: e.target,
        sourceHandle: e.sourceHandle ?? null,
        targetHandle: e.targetHandle ?? null,
        data: e.data,
    }));
    // Canvas notes are in the key too: without them a note would never be a step
    // of its own, and the next undo would take it back with whatever came before.
    const notes = (s.notes ?? []).map(n => ({
        id: n.id,
        x: Math.round(n.position?.x ?? 0),
        y: Math.round(n.position?.y ?? 0),
        text: (n.data as { text?: unknown } | undefined)?.text,
    }));
    return JSON.stringify({ nodes, edges, notes });
}

type Stack = { past: CanvasSnapshot[]; future: CanvasSnapshot[] };
type Pending = {
    job: string;
    prev: CanvasSnapshot;
    settled: CanvasSnapshot;
    /** Recorded because noteEdit said so, not because the key changed. */
    noted: boolean;
    cancel: () => void;
};

export class UndoHistory {
    private stacks: Record<string, Stack> = {};
    // Last *recorded* snapshot per job (the baseline the next change is diffed
    // against and what we push to `past`).
    private baseline: Record<string, CanvasSnapshot> = {};
    private pending: Pending | null = null;
    private suppress = false; // true while applying an undo/redo
    private editNoted = false;
    private scope: string | undefined;
    private job: string;
    private latest: CanvasSnapshot;

    constructor(
        job: string,
        initial: CanvasSnapshot,
        private readonly timer: Timer,
        private readonly changed: () => void,
    ) {
        this.job = job;
        this.latest = initial;
    }

    private stackFor(job: string): Stack {
        if (!this.stacks[job]) this.stacks[job] = { past: [], future: [] };
        return this.stacks[job];
    }

    /**
     * The active pipeline as it is now, in `scope` - the workspace it belongs to.
     *
     * History was keyed by pipeline id alone, and ids repeat across workspaces
     * (every new one starts at j1), so after a switch Ctrl+Z put the other
     * workspace's pipeline on this canvas and autosave wrote it. A new scope
     * starts with no history; an edit still in its debounce belonged to the old
     * one and is dropped with it.
     */
    observe(job: string, snapshot: CanvasSnapshot, scope = ''): void {
        this.latest = snapshot;
        const noted = this.editNoted;
        this.editNoted = false;

        if (this.scope !== scope) {
            this.pending?.cancel();
            this.pending = null;
            this.stacks = {};
            this.baseline = {};
            this.suppress = false;
            this.scope = scope;
            this.job = job;
            this.baseline[job] = snapshot;
            this.changed();
            return;
        }

        // Pipeline switched: re-baseline, never record across pipelines. An edit
        // still inside its debounce belongs to the pipeline it was made in, so
        // it is recorded there first rather than dropped.
        if (this.job !== job) {
            this.flush();
            this.job = job;
            this.baseline[job] = snapshot;
            this.stackFor(job);
            this.changed();
            return;
        }
        // A burst of changes is one step: the new one replaces the pending one,
        // which still diffs against the same baseline.
        const carried = this.pending;
        carried?.cancel();
        this.pending = null;
        // This change came from our own undo/redo apply: re-baseline, skip.
        if (this.suppress) {
            this.suppress = false;
            this.baseline[job] = snapshot;
            this.changed();
            return;
        }
        const base = this.baseline[job];
        if (base === undefined) {
            this.baseline[job] = snapshot;
            return;
        }
        const isNoted = noted || !!carried?.noted;
        if (!isNoted && meaningfulKey(base) === meaningfulKey(snapshot)) {
            return; // selection / run-preview only -> not undoable
        }
        this.pending = {
            job,
            prev: base,
            settled: snapshot,
            noted: isNoted,
            cancel: this.timer(() => this.commit(), DEBOUNCE_MS),
        };
    }

    /**
     * The next observed change is a user edit even though the key cannot see it.
     *
     * A declared schema lives in `data.schema`, the same field a run writes its
     * output columns to, so the key has to leave it out or every run would be a
     * step. The one place a person edits it says so here instead.
     */
    noteEdit(): void {
        this.editNoted = true;
    }

    /** Record an edit still inside its debounce now, instead of losing it. */
    private flush(): void {
        if (!this.pending) return;
        this.pending.cancel();
        this.commit();
    }

    private commit(): void {
        const p = this.pending;
        if (!p) return;
        this.pending = null;
        const st = this.stackFor(p.job);
        st.past.push(p.prev);
        if (st.past.length > HISTORY_LIMIT) st.past.shift();
        st.future = [];
        this.baseline[p.job] = p.settled;
        this.changed();
    }

    /** The snapshot to apply for an undo, or null when there is nothing to undo. */
    undo(): CanvasSnapshot | null {
        // An edit still inside its debounce is the newest step. Without this, an
        // undo pressed within 350 ms of an edit went back past it, and that
        // state could never be reached again.
        this.flush();
        const st = this.stackFor(this.job);
        if (!st.past.length) return null;
        const restore = st.past.pop()!;
        st.future.push(this.latest);
        this.suppress = true;
        this.baseline[this.job] = restore;
        this.changed();
        return restore;
    }

    /** The snapshot to apply for a redo, or null when there is nothing to redo. */
    redo(): CanvasSnapshot | null {
        // A pending edit is a new branch, and recording it clears what redo had.
        this.flush();
        const st = this.stackFor(this.job);
        if (!st.future.length) return null;
        const restore = st.future.pop()!;
        st.past.push(this.latest);
        this.suppress = true;
        this.baseline[this.job] = restore;
        this.changed();
        return restore;
    }

    canUndo(): boolean {
        return this.pending?.job === this.job || (this.stacks[this.job]?.past.length ?? 0) > 0;
    }

    canRedo(): boolean {
        return (this.stacks[this.job]?.future.length ?? 0) > 0;
    }

    /** Stop a pending debounce; the hook is unmounting. */
    dispose(): void {
        this.pending?.cancel();
        this.pending = null;
    }
}
