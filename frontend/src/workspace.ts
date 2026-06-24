import { isTauri } from './tauri-dialog';
import { isWebBackend, webFs } from './web-fs';

// A backend that can read/write the workspace exists in the desktop app (Tauri
// fs) and in the web edition (HTTP fs via duckle-runner). Browser dev with no
// server has neither, so file ops stay inert there.
function hasBackend(): boolean {
    return isTauri() || isWebBackend();
}

const WORKSPACE_PATH_KEY = 'duckle:workspace-path';

// Workspace v1 (single file, everything in one blob). Kept for the
// migration path.
const V1_FILE = 'workspace.json';
// Workspace v2 (this commit).
const METADATA_FILE = 'duckle.json';
const REPOSITORY_FILE = 'repository.json';
const PIPELINES_DIR = 'pipelines';
const CONNECTIONS_DIR = 'connections';
const CONTEXTS_DIR = 'contexts';
const ROUTINES_DIR = 'routines';
const DOCS_DIR = 'docs';
const DIVES_DIR = 'dives';
const DASHBOARDS_DIR = 'dashboards';

const PAYLOAD_DIR_BY_TYPE: Record<string, string> = {
    pipeline: PIPELINES_DIR,
    connection: CONNECTIONS_DIR,
    context: CONTEXTS_DIR,
    routine: ROUTINES_DIR,
    doc: DOCS_DIR,
    dive: DIVES_DIR,
    dashboard: DASHBOARDS_DIR,
};

export type WorkspaceState = {
    version: number;
    engine?: string;
    pipelineData?: Record<string, unknown>;
    repo?: unknown[];
    jobs?: unknown[];
    activeJobId?: string;
    // Paths of per-item files (contexts, connections, pipelines) that failed
    // to parse. The rest of the workspace still loaded; these are reported so
    // the UI can warn instead of failing silently.
    corruptFiles?: string[];
};

/**
 * Thrown when a workspace file exists but contains invalid JSON (e.g. it was
 * hand-edited outside the app). Carries the offending file path so the UI can
 * tell the user exactly what to fix or restore, rather than silently showing
 * an empty workspace.
 */
export class WorkspaceLoadError extends Error {
    constructor(
        public readonly file: string,
        public readonly reason: string,
    ) {
        super(`Invalid JSON in ${file}: ${reason}`);
        this.name = 'WorkspaceLoadError';
    }
}

export function isInTauri(): boolean {
    return isTauri();
}

export function getWorkspacePath(): string | null {
    try {
        return localStorage.getItem(WORKSPACE_PATH_KEY);
    } catch {
        return null;
    }
}

export function setWorkspacePath(path: string): void {
    try {
        localStorage.setItem(WORKSPACE_PATH_KEY, path);
    } catch {
        /* ignore */
    }
}

export function clearWorkspacePath(): void {
    try {
        localStorage.removeItem(WORKSPACE_PATH_KEY);
    } catch {
        /* ignore */
    }
}

function joinPath(dir: string, ...parts: string[]): string {
    const sep = dir.includes('\\') && !dir.includes('/') ? '\\' : '/';
    return [dir.replace(/[/\\]+$/, ''), ...parts].join(sep);
}

export async function pickWorkspaceDirectory(): Promise<string | null> {
    if (!isTauri()) return null;
    try {
        const { open } = await import('@tauri-apps/plugin-dialog');
        const result = await open({
            directory: true,
            multiple: false,
            title: 'Choose Duckle workspace folder',
        });
        return typeof result === 'string' ? result : null;
    } catch (err) {
        console.error('Workspace picker failed', err);
        return null;
    }
}

type FsLib = typeof import('@tauri-apps/plugin-fs');

async function fs(): Promise<FsLib> {
    // Desktop -> the native fs plugin; web edition -> the HTTP fs shim. The web
    // shim implements the subset of the plugin that this module uses.
    if (!isTauri()) {
        return webFs as unknown as FsLib;
    }
    return await import('@tauri-apps/plugin-fs');
}

// Encrypt / decrypt a connection payload's sensitive fields (password, tokens,
// keys) via the desktop crypto commands, which use a per-workspace key under
// `.duckle/keys/`. On any error we fall back to the original payload so a save
// never loses data and a load never blocks. `${...}` placeholders and
// non-secret fields are left untouched by the command.
async function encryptConnectionPayload(workspace: string, payload: unknown): Promise<unknown> {
    try {
        const { invoke } = await import('@tauri-apps/api/core');
        const enc = await invoke<string>('connection_encrypt_payload', {
            workspace,
            payloadJson: JSON.stringify(payload),
        });
        return JSON.parse(enc);
    } catch (err) {
        console.error('encrypt connection failed', err);
        return payload;
    }
}

async function decryptConnectionPayload(workspace: string, payload: unknown): Promise<unknown> {
    try {
        const { invoke } = await import('@tauri-apps/api/core');
        const dec = await invoke<string>('connection_decrypt_payload', {
            workspace,
            payloadJson: JSON.stringify(payload),
        });
        return JSON.parse(dec);
    } catch (err) {
        console.error('decrypt connection failed', err);
        return payload;
    }
}

async function ensureDir(path: string): Promise<void> {
    const { exists, mkdir } = await fs();
    if (!(await exists(path))) {
        await mkdir(path, { recursive: true });
    }
}

async function writeJson(path: string, value: unknown): Promise<void> {
    const { writeTextFile } = await fs();
    await writeTextFile(path, JSON.stringify(value, null, 2));
}

async function readJsonIfExists<T = unknown>(path: string): Promise<T | null> {
    const { exists, readTextFile } = await fs();
    if (!(await exists(path))) return null;
    const content = await readTextFile(path);
    try {
        return JSON.parse(content) as T;
    } catch (err) {
        // A present-but-unparseable file is a corruption, not an absence. Make
        // it distinguishable so callers can skip-and-report (per item) or
        // surface a hard error (critical files), never treat it as "empty".
        throw new WorkspaceLoadError(path, err instanceof Error ? err.message : String(err));
    }
}

async function readDirEntries(path: string): Promise<string[]> {
    try {
        const { exists, readDir } = await fs();
        if (!(await exists(path))) return [];
        const entries = await readDir(path);
        return entries
            .filter(e => e.isFile && e.name.endsWith('.json'))
            .map(e => e.name);
    } catch {
        return [];
    }
}

// ---- Load (with migration) ---------------------------------------------

/**
 * Load the workspace from disk. Reads the v2 multi-file layout if it
 * exists; otherwise tries to migrate a v1 workspace.json on the fly.
 * Returns `null` only if there's nothing to load (fresh workspace) or
 * we're running in browser mode.
 */
export async function loadWorkspace(path: string): Promise<WorkspaceState | null> {
    if (!hasBackend()) return null;
    try {
        const v2 = await loadV2(path);
        if (v2) return v2;
        const v1 = await loadAndMigrateV1(path);
        if (v1) return v1;
        return null;
    } catch (err) {
        // A structural file (duckle.json / repository.json) is corrupt: surface
        // it so the caller can show a hard error and, crucially, NOT fall back
        // to a default in-memory state that the auto-save would then write over
        // the still-good files on disk.
        if (err instanceof WorkspaceLoadError) throw err;
        console.error('Failed to load workspace', err);
        return null;
    }
}

async function loadV2(path: string): Promise<WorkspaceState | null> {
    const meta = await readJsonIfExists<{
        version?: number;
        engine?: string;
        jobs?: unknown[];
        activeJobId?: string;
    }>(joinPath(path, METADATA_FILE));
    if (!meta) return null;

    const repo = (await readJsonIfExists<Array<Record<string, unknown>>>(
        joinPath(path, REPOSITORY_FILE),
    )) ?? [];

    // Per-item files (contexts, connections, pipelines) are loaded
    // independently: a single corrupt one is skipped and reported, never
    // allowed to abort the whole workspace load. (`duckle.json` and
    // `repository.json` above are structural - if those are corrupt the
    // WorkspaceLoadError propagates so the caller can show a hard error.)
    const corruptFiles: string[] = [];

    // Hydrate payloads for each repo item that lives in its own file.
    for (const item of repo) {
        const itype = typeof item.type === 'string' ? item.type : '';
        const dir = PAYLOAD_DIR_BY_TYPE[itype];
        if (!dir || itype === 'pipeline' || itype === 'folder' || itype === 'project') continue;
        const file = joinPath(path, dir, `${item.id}.json`);
        try {
            const payload = await readJsonIfExists(file);
            if (payload !== null) {
                (item as { payload: unknown }).payload =
                    itype === 'connection' ? await decryptConnectionPayload(path, payload) : payload;
            }
        } catch (err) {
            if (err instanceof WorkspaceLoadError) corruptFiles.push(err.file);
            else throw err;
        }
    }

    // Load each pipeline file referenced in the repo.
    const pipelineData: Record<string, unknown> = {};
    for (const item of repo) {
        if (item.type !== 'pipeline') continue;
        const file = joinPath(path, PIPELINES_DIR, `${item.id}.json`);
        try {
            const pipeline = await readJsonIfExists(file);
            if (pipeline) pipelineData[item.id as string] = pipeline;
        } catch (err) {
            if (err instanceof WorkspaceLoadError) corruptFiles.push(err.file);
            else throw err;
        }
    }

    return {
        version: meta.version ?? 2,
        engine: meta.engine,
        jobs: meta.jobs,
        activeJobId: meta.activeJobId,
        repo,
        pipelineData,
        corruptFiles: corruptFiles.length ? corruptFiles : undefined,
    };
}

async function loadAndMigrateV1(path: string): Promise<WorkspaceState | null> {
    const v1Path = joinPath(path, V1_FILE);
    const v1 = await readJsonIfExists<WorkspaceState>(v1Path);
    if (!v1) return null;
    // Write v2 files alongside; archive v1.
    try {
        await saveAll(path, v1);
        const { writeTextFile, exists, remove } = await fs();
        const backup = joinPath(path, `${V1_FILE}.v1.bak`);
        await writeTextFile(backup, JSON.stringify(v1, null, 2));
        // saveAll's callees swallow their own errors, so verify the v2 files
        // actually landed before deleting v1.json - otherwise a partial write
        // would orphan the data (loadV2 reads duckle.json + repository.json,
        // not the .bak), leaving the workspace looking empty.
        const metaOk = (await readJsonIfExists(joinPath(path, METADATA_FILE))) !== null;
        const repoOk = (await readJsonIfExists(joinPath(path, REPOSITORY_FILE))) !== null;
        if (metaOk && repoOk) {
            if (await exists(v1Path)) {
                try {
                    await remove(v1Path);
                } catch {
                    /* leave it if we can't remove */
                }
            }
            console.info('Migrated workspace from v1 -> v2');
        } else {
            console.warn('Migration incomplete (v2 files missing); kept workspace.json');
        }
    } catch (err) {
        console.warn('Migration failed; loading v1 in-memory only', err);
    }
    return v1;
}

// ---- Save (granular) ---------------------------------------------------

/**
 * Write the metadata file only - cheap; safe to call on every change.
 */
export async function saveMetadata(
    path: string,
    metadata: { engine?: string; jobs?: unknown; activeJobId?: string },
): Promise<void> {
    if (!hasBackend()) return;
    try {
        await ensureDir(path);
        await writeJson(joinPath(path, METADATA_FILE), {
            version: 2,
            ...metadata,
        });
    } catch (err) {
        console.error('saveMetadata failed', err);
    }
}

/**
 * Write the repository tree (id, name, type, parentId, icon). Payloads
 * live in their own per-type directories.
 */
export async function saveRepository(
    path: string,
    items: Array<Record<string, unknown>>,
): Promise<void> {
    if (!hasBackend()) return;
    try {
        await ensureDir(path);
        const stripped = items.map(i => {
            const { payload, ...rest } = i as Record<string, unknown> & { payload?: unknown };
            void payload;
            return rest;
        });
        await writeJson(joinPath(path, REPOSITORY_FILE), stripped);
    } catch (err) {
        console.error('saveRepository failed', err);
    }
}

export async function savePipelineFile(
    path: string,
    pipelineId: string,
    pipeline: unknown,
): Promise<boolean> {
    if (!hasBackend()) return true;
    try {
        const dir = joinPath(path, PIPELINES_DIR);
        await ensureDir(dir);
        await writeJson(joinPath(dir, `${pipelineId}.json`), pipeline);
        return true;
    } catch (err) {
        console.error('savePipelineFile failed', err);
        return false;
    }
}

export async function saveItemPayload(
    path: string,
    itemType: string,
    itemId: string,
    payload: unknown,
): Promise<boolean> {
    if (!hasBackend()) return true;
    const dir = PAYLOAD_DIR_BY_TYPE[itemType];
    if (!dir) return true;
    try {
        const folder = joinPath(path, dir);
        await ensureDir(folder);
        const toWrite =
            itemType === 'connection' ? await encryptConnectionPayload(path, payload) : payload;
        await writeJson(joinPath(folder, `${itemId}.json`), toWrite);
        return true;
    } catch (err) {
        console.error('saveItemPayload failed', err);
        return false;
    }
}

export async function deletePipelineFile(
    path: string,
    pipelineId: string,
): Promise<void> {
    if (!hasBackend()) return;
    try {
        const { exists, remove } = await fs();
        const file = joinPath(path, PIPELINES_DIR, `${pipelineId}.json`);
        if (await exists(file)) await remove(file);
    } catch (err) {
        console.warn('deletePipelineFile failed', err);
    }
}

export async function deleteItemPayload(
    path: string,
    itemType: string,
    itemId: string,
): Promise<void> {
    if (!hasBackend()) return;
    const dir = PAYLOAD_DIR_BY_TYPE[itemType];
    if (!dir) return;
    try {
        const { exists, remove } = await fs();
        const file = joinPath(path, dir, `${itemId}.json`);
        if (await exists(file)) await remove(file);
    } catch (err) {
        console.warn('deleteItemPayload failed', err);
    }
}

/**
 * Convenience: write the full workspace state in v2 layout. Used by
 * migration and as a fallback.
 */
export async function saveAll(path: string, state: WorkspaceState): Promise<void> {
    if (!hasBackend()) return;
    await ensureDir(path);
    await saveMetadata(path, {
        engine: state.engine,
        jobs: state.jobs,
        activeJobId: state.activeJobId,
    });
    if (Array.isArray(state.repo)) {
        await saveRepository(path, state.repo as Array<Record<string, unknown>>);
        for (const item of state.repo as Array<Record<string, unknown>>) {
            const itype = typeof item.type === 'string' ? item.type : '';
            if (itype === 'pipeline' || itype === 'folder' || itype === 'project') continue;
            const payload = (item as { payload?: unknown }).payload;
            if (payload !== undefined) {
                await saveItemPayload(path, itype, item.id as string, payload);
            }
        }
    }
    if (state.pipelineData) {
        for (const [id, pipeline] of Object.entries(state.pipelineData)) {
            await savePipelineFile(path, id, pipeline);
        }
    }
}

// Kept for backwards compatibility - callers that just want to write
// everything in one shot can still call saveWorkspace().
export const saveWorkspace = saveAll;

// Expose for cleanup utilities.
export async function listPipelineFiles(path: string): Promise<string[]> {
    return readDirEntries(joinPath(path, PIPELINES_DIR));
}
