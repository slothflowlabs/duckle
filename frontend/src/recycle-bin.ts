// The recycle bin: a removed item's file and its place in the project tree,
// kept under .duckle/trash so it can be put back. The functions here are pure;
// workspace.ts reads and writes the files on both the desktop and the web.

/** A project-tree item as the bin keeps it: its tree fields, never its payload. */
export type TrashItem = {
    id: string;
    name: string;
    type: string;
    parentId?: string | null;
    [key: string]: unknown;
};

/** One removed item: its tree entry, the file it had (relative to the
 * workspace, or null for a folder) and that file's text. */
export type TrashEntry = {
    batch: string;
    deletedAt: string;
    item: TrashItem;
    file: string | null;
    content: string | null;
};

/** The items removed together, which go back together. `heads` are the ones
 * whose parent is not in the batch: what the person deleted. */
export type TrashBatch = {
    batch: string;
    deletedAt: string;
    entries: TrashEntry[];
    heads: TrashItem[];
};

/** The file an entry is kept in under .duckle/trash. */
export function trashFileName(batch: string, id: string): string {
    return `${batch}__${id}.json`;
}

/** An entry for a removed item. The in-memory payload is left out: the file's
 * text is what goes back, exactly as it was written (a connection's secrets
 * stay encrypted). */
export function trashEntry(
    item: TrashItem,
    batch: string,
    deletedAt: string,
    file: string | null,
    content: string | null,
): TrashEntry {
    const kept: TrashItem = { ...item };
    delete kept.payload;
    return { batch, deletedAt, item: kept, file, content };
}

/** Entries grouped by the save that removed them, newest first. */
export function batchesOf(entries: TrashEntry[]): TrashBatch[] {
    const byBatch = new Map<string, TrashEntry[]>();
    for (const e of entries) {
        const list = byBatch.get(e.batch) ?? [];
        list.push(e);
        byBatch.set(e.batch, list);
    }
    const batches: TrashBatch[] = [];
    for (const [batch, list] of byBatch) {
        const ids = new Set(list.map(e => e.item.id));
        batches.push({
            batch,
            deletedAt: list[0].deletedAt,
            entries: list,
            heads: list.filter(e => !e.item.parentId || !ids.has(e.item.parentId)).map(e => e.item),
        });
    }
    return batches.sort((x, y) => y.deletedAt.localeCompare(x.deletedAt) || y.batch.localeCompare(x.batch));
}

/** What a batch holds, as the bin lists it: `folder "Sales" and 1 nested item`. */
export function describeBatch(batch: TrashBatch): string {
    const head = batch.heads[0] ?? batch.entries[0]?.item;
    if (!head) return 'nothing';
    const named = `${head.type} "${head.name}"`;
    const rest = batch.entries.length - 1;
    if (rest === 0) return named;
    if (batch.heads.length > 1) return `${named} and ${rest} other item${rest === 1 ? '' : 's'}`;
    return `${named} and ${rest} nested item${rest === 1 ? '' : 's'}`;
}

/** The project tree with `entries` put back. An item keeps its folder when that
 * folder is still there or comes back with it; otherwise it goes under the
 * project. An item already in the tree is left as it is. */
export function restoredRepo(repo: TrashItem[], entries: TrashEntry[]): TrashItem[] {
    const present = new Set(repo.map(i => i.id));
    const returning = new Set(entries.map(e => e.item.id));
    const project = repo.find(i => i.type === 'project')?.id ?? null;
    const added: TrashItem[] = [];
    for (const e of entries) {
        if (present.has(e.item.id)) continue;
        const parent = e.item.parentId ?? null;
        const parentStays = parent !== null && (present.has(parent) || returning.has(parent));
        added.push({ ...e.item, parentId: parentStays ? parent : project });
        present.add(e.item.id);
    }
    return [...repo, ...added];
}
