import { Channel, invoke } from '@tauri-apps/api/core';
import { isTauri } from './tauri-dialog';
import { isWebBackend } from './web-fs';
import type { Column } from './pipeline-types';
import type { Edge, Node } from '@xyflow/react';
import type { DuckleNodeData } from './pipeline-types';

type AutodetectPayload = {
    columns: Column[];
    sampleRows: Record<string, unknown>[];
};

/**
 * Call into the Rust `autodetect_schema` Tauri command when running
 * under Tauri. Returns `null` in browser mode or on failure, so the
 * caller can fall back to a mock.
 */
export async function tauriAutodetect(
    format: string,
    options: Record<string, unknown>,
): Promise<AutodetectPayload | null> {
    if (!isTauri()) return null;
    try {
        return await invoke<AutodetectPayload>('autodetect_schema', { format, options });
    } catch (err) {
        console.warn('Tauri autodetect failed for ' + format, err);
        return null;
    }
}

// ---- Pipeline execution ------------------------------------------------

export type NodeRunStatus = {
    status: 'ok' | 'error' | 'running';
    kind?: 'view' | 'sink';
    rows?: number;
    duration_ms?: number;
    error?: string;
    /** Coarse error bucket (auth/network/timeout/oom/disk/schema/syntax/
     *  cancelled/other) - present only when `error` is. */
    category?: string;
    /** The compiled SQL statement that failed (present only on error), so any
     *  component's failure shows exactly what ran. */
    sql?: string;
};

export type NodePreview = {
    node_id: string;
    columns: Column[];
    rows: Record<string, unknown>[];
};

export type RunLogLine = {
    node_id: string;
    level: 'info' | 'warn' | 'error';
    message: string;
};

export type RunResult = {
    status: 'ok' | 'error' | 'cancelled';
    duration_ms: number;
    nodes: Record<string, NodeRunStatus>;
    preview: NodePreview[];
    error?: string;
    /** Coarse bucket of `error` (see NodeRunStatus.category). */
    category?: string;
    /** Diagnostic lines from ctl.log / ctl.warn nodes, accumulated live
     *  from streamed `log` events (not part of the engine's RunResult). */
    messages?: RunLogLine[];
};

export type PipelineEvent =
    | { type: 'started'; total_stages: number }
    | { type: 'stage_started'; node_id: string; label: string; kind: 'view' | 'sink' }
    | {
          type: 'stage_finished';
          node_id: string;
          kind: 'view' | 'sink';
          status: 'ok' | 'error';
          rows?: number;
          duration_ms: number;
          error?: string;
          sql?: string;
      }
    | { type: 'cancelled' }
    | { type: 'log'; node_id: string; level: 'info' | 'warn' | 'error'; message: string }
    | { type: 'finished'; status: 'ok' | 'error' | 'cancelled'; duration_ms: number };

/**
 * Web edition run transport: POST the pipeline to /api/run_stream and read the
 * Server-Sent Events the runner emits - each `data:` line is a PipelineEvent
 * (fed to onEvent for the live per-node animation), the final `event: result`
 * line carries the RunResult. Mirrors the desktop Channel without Tauri.
 */
async function runViaSse(
    pipeline: { nodes: Node<DuckleNodeData>[]; edges: Edge[] },
    onEvent?: (evt: PipelineEvent) => void,
    pipelineId?: string,
    pipelineName?: string | null,
    workspacePath?: string | null,
    targetNodeId?: string,
): Promise<RunResult | null> {
    const fail = (error: string): RunResult => ({
        status: 'error',
        duration_ms: 0,
        nodes: {},
        preview: [],
        error,
    });
    try {
        const res = await fetch('/api/run_stream', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                pipeline,
                pipelineId: pipelineId ?? null,
                pipelineName: pipelineName ?? null,
                workspacePath: workspacePath ?? null,
                // Present for run-to-here (partial); omitted/null = full run.
                targetNodeId: targetNodeId ?? null,
            }),
        });
        if (!res.ok || !res.body) return fail('run failed: HTTP ' + res.status);
        const reader = res.body.getReader();
        const decoder = new TextDecoder();
        let buf = '';
        let result: RunResult | null = null;
        for (;;) {
            const { done, value } = await reader.read();
            if (done) break;
            buf += decoder.decode(value, { stream: true });
            // SSE frames are separated by a blank line.
            let sep: number;
            while ((sep = buf.indexOf('\n\n')) >= 0) {
                const frame = buf.slice(0, sep);
                buf = buf.slice(sep + 2);
                let isResult = false;
                let data = '';
                for (const line of frame.split('\n')) {
                    if (line.startsWith('event:')) isResult = line.slice(6).trim() === 'result';
                    else if (line.startsWith('data:')) data += line.slice(5).trim();
                }
                if (!data) continue;
                try {
                    const obj = JSON.parse(data);
                    if (isResult) result = obj as RunResult;
                    else onEvent?.(obj as PipelineEvent);
                } catch {
                    // ignore a malformed frame
                }
            }
        }
        return result ?? fail('run produced no result');
    } catch (err) {
        return fail(String(err));
    }
}

export async function runPipeline(
    nodes: Node<DuckleNodeData>[],
    edges: Edge[],
    onEvent?: (evt: PipelineEvent) => void,
    pipelineId?: string,
    workspacePath?: string | null,
    pipelineName?: string | null,
): Promise<RunResult | null> {
    if (!isTauri() && !isWebBackend()) return null;
    // Web edition streams progress over SSE so the live per-node animation works
    // just like the desktop Channel.
    if (isWebBackend()) {
        return runViaSse({ nodes, edges }, onEvent, pipelineId, pipelineName, workspacePath);
    }
    const channel = new Channel<PipelineEvent>();
    if (onEvent) channel.onmessage = onEvent;
    try {
        return await invoke<RunResult>('run_pipeline', {
            pipeline: { nodes, edges },
            onEvent: channel,
            pipelineId: pipelineId ?? null,
            pipelineName: pipelineName ?? null,
            workspacePath: workspacePath ?? null,
        });
    } catch (err) {
        console.error('runPipeline failed', err);
        return {
            status: 'error',
            duration_ms: 0,
            nodes: {},
            preview: [],
            error: String(err),
        };
    }
}

export async function runPipelinePartial(
    nodes: Node<DuckleNodeData>[],
    edges: Edge[],
    targetNodeId: string,
    onEvent?: (evt: PipelineEvent) => void,
    pipelineId?: string,
    workspacePath?: string | null,
    pipelineName?: string | null,
): Promise<RunResult | null> {
    if (!isTauri() && !isWebBackend()) return null;
    // Web edition: run-to-here streams over SSE like a full run, passing the
    // target node so the server runs only the subgraph up to it.
    if (isWebBackend()) {
        return runViaSse({ nodes, edges }, onEvent, pipelineId, pipelineName, workspacePath, targetNodeId);
    }
    const channel = new Channel<PipelineEvent>();
    if (onEvent) channel.onmessage = onEvent;
    try {
        return await invoke<RunResult>('run_pipeline_partial', {
            pipeline: { nodes, edges },
            targetNodeId,
            onEvent: channel,
            pipelineId: pipelineId ?? null,
            pipelineName: pipelineName ?? null,
            workspacePath: workspacePath ?? null,
        });
    } catch (err) {
        console.error('runPipelinePartial failed', err);
        return {
            status: 'error',
            duration_ms: 0,
            nodes: {},
            preview: [],
            error: String(err),
        };
    }
}

export type RunRecord = {
    at: string;
    status: string;
    duration_ms: number;
    rows: number;
    node_count: number;
    trigger: string;
    error?: string;
    /** Coarse error bucket (auth/network/timeout/oom/disk/schema/syntax/...). */
    category?: string;
};

export async function runHistory(
    workspacePath: string,
    pipelineId: string,
): Promise<RunRecord[]> {
    if (!isTauri()) return [];
    try {
        return await invoke<RunRecord[]>('run_history', {
            workspacePath,
            pipelineId,
        });
    } catch (err) {
        console.warn('runHistory failed', err);
        return [];
    }
}

// ---- Backfill: xf.incremental / src.ducklake.changes saved state -------

export type WatermarkEntry = {
    node_id: string;
    /** "incremental" (value + value_type) or "snapshot" (DuckLake CDC). */
    kind: string;
    value: string;
    value_type?: string;
};

export async function watermarkList(
    workspacePath: string,
    pipelineName: string,
): Promise<WatermarkEntry[]> {
    if (!isTauri()) return [];
    try {
        return await invoke<WatermarkEntry[]>('watermark_list', {
            workspacePath,
            pipelineName,
        });
    } catch (err) {
        console.warn('watermarkList failed', err);
        return [];
    }
}

export async function watermarkSet(
    workspacePath: string,
    pipelineName: string,
    nodeId: string,
    kind: string,
    value: string,
    valueType?: string,
): Promise<void> {
    if (!isTauri()) return;
    await invoke('watermark_set', {
        workspacePath,
        pipelineName,
        nodeId,
        kind,
        value,
        valueType,
    });
}

export async function watermarkClear(
    workspacePath: string,
    pipelineName: string,
    nodeId: string,
): Promise<void> {
    if (!isTauri()) return;
    await invoke('watermark_clear', { workspacePath, pipelineName, nodeId });
}

// ---- Engine install (first-run guided setup) ---------------------------

export type EngineStatus = {
    id: string;
    name: string;
    description: string;
    required: boolean;
    installed: boolean;
    /** Version currently on disk (undefined when no binary is present). */
    version?: string;
    /** Version this build of Duckle pins/ships. */
    target_version: string;
    /** A binary is present but a different version - an upgrade is available. */
    outdated: boolean;
    path?: string;
    available: boolean;
};

export type InstallProgress =
    | { phase: 'downloading'; received: number; total?: number }
    | { phase: 'extracting' }
    | { phase: 'verifying' }
    | { phase: 'installing_extension'; name: string; index: number; total: number }
    // llamacpp only: separate progress phase for the Qwen GGUF model
    // (~1.1 GB, much larger than the binary itself).
    | { phase: 'downloading_model'; received: number; total?: number }
    | { phase: 'done'; path: string }
    // Set by the frontend on a caught install error (the Rust command
    // returns Err rather than streaming this).
    | { phase: 'failed'; error: string };

export async function engineStatus(): Promise<EngineStatus[]> {
    if (!isTauri()) return [];
    try {
        return await invoke<EngineStatus[]>('engine_status');
    } catch (err) {
        console.warn('engineStatus failed', err);
        return [];
    }
}

export async function engineInstall(
    engine: string,
    onProgress?: (p: InstallProgress) => void,
): Promise<string> {
    const channel = new Channel<InstallProgress>();
    if (onProgress) channel.onmessage = onProgress;
    return await invoke<string>('engine_install', { engine, onProgress: channel });
}

// ---- AI Chat (local Qwen via llama-server) -----------------------------

export type ChatMessage = { role: 'user' | 'assistant' | 'system'; content: string };

export type ChatEvent =
    | { kind: 'token'; text: string }
    | { kind: 'done' }
    | { kind: 'error'; message: string };

/**
 * Send a chat conversation to the local Qwen model. Tokens stream
 * back via `onEvent`. The system prompt is added by the backend.
 */
export async function chatSend(
    history: ChatMessage[],
    onEvent: (e: ChatEvent) => void,
    workspace?: string | null,
): Promise<void> {
    if (!isTauri()) {
        onEvent({ kind: 'error', message: 'Chat is only available in the desktop app.' });
        return;
    }
    const channel = new Channel<ChatEvent>();
    channel.onmessage = onEvent;
    try {
        // workspace lets the backend route to an external AI endpoint if configured (#92).
        await invoke('chat_send', { history, onEvent: channel, workspace: workspace ?? null });
    } catch (err) {
        onEvent({ kind: 'error', message: String(err) });
    }
}

/**
 * Pull a Duckle pipeline JSON out of an assistant message - the
 * model is asked to wrap pipelines in ```json fenced code blocks.
 * Returns null if no extractable pipeline.
 */
export async function chatExtractPipeline(text: string): Promise<unknown | null> {
    if (!isTauri()) return null;
    try {
        return await invoke('chat_extract_pipeline', { text });
    } catch {
        return null;
    }
}

// ---- In-app Git integration --------------------------------------------

export type ChangedFile = {
    path: string;
    status: 'staged' | 'modified' | 'untracked' | 'conflicted' | 'deleted' | 'renamed';
};

export type GitRemote = {
    name: string;
    url: string;
    provider: 'github' | 'gitlab' | 'bitbucket' | 'other';
};

export type GitStatus = {
    initialized: boolean;
    branch: string | null;
    ahead: number;
    behind: number;
    remote: GitRemote | null;
    files: ChangedFile[];
    has_pat: boolean;
};

export type CiState =
    | 'success'
    | 'failure'
    | 'in_progress'
    | 'pending'
    | 'cancelled'
    | 'none'
    | 'unknown';

export type CiStatus = {
    provider: 'github' | 'gitlab' | 'unknown';
    state: CiState;
    label: string;
    url: string | null;
    sha: string | null;
};

export async function workspaceGitStatus(workspacePath: string): Promise<GitStatus | null> {
    if (!isTauri() || !workspacePath) return null;
    try {
        return await invoke<GitStatus>('workspace_git_status', { workspacePath });
    } catch (err) {
        console.warn('workspace_git_status:', err);
        return null;
    }
}

export async function workspaceGitInit(workspacePath: string): Promise<void> {
    await invoke('workspace_git_init', { workspacePath });
}

export async function workspaceGitCommit(
    workspacePath: string,
    message: string,
): Promise<string> {
    return await invoke<string>('workspace_git_commit', { workspacePath, message });
}

/** Returns 'AUTH_REQUIRED' (as Error.message prefix) when a PAT is needed. */
export async function workspaceGitPush(workspacePath: string): Promise<string> {
    return await invoke<string>('workspace_git_push', { workspacePath });
}

export async function workspaceGitPull(workspacePath: string): Promise<string> {
    return await invoke<string>('workspace_git_pull', { workspacePath });
}

export async function workspaceGitBranches(workspacePath: string): Promise<string[]> {
    return await invoke<string[]>('workspace_git_branches', { workspacePath });
}

export async function workspaceGitBranchCreate(
    workspacePath: string,
    name: string,
): Promise<void> {
    await invoke('workspace_git_branch_create', { workspacePath, name });
}

export async function workspaceGitBranchCheckout(
    workspacePath: string,
    name: string,
): Promise<void> {
    await invoke('workspace_git_branch_checkout', { workspacePath, name });
}

export async function workspaceGitRemoteSet(
    workspacePath: string,
    url: string,
): Promise<void> {
    await invoke('workspace_git_remote_set', { workspacePath, url });
}

export async function workspaceGitSavePat(
    workspacePath: string,
    token: string,
): Promise<void> {
    await invoke('workspace_git_save_pat', { workspacePath, token });
}

export async function workspaceGitClearPat(workspacePath: string): Promise<void> {
    await invoke('workspace_git_clear_pat', { workspacePath });
}

export async function workspaceCiStatus(workspacePath: string): Promise<CiStatus | null> {
    if (!isTauri() || !workspacePath) return null;
    try {
        return await invoke<CiStatus>('workspace_ci_status', { workspacePath });
    } catch (err) {
        console.warn('workspace_ci_status:', err);
        return null;
    }
}

export async function cancelPipeline(): Promise<void> {
    if (!isTauri()) return;
    try {
        await invoke('cancel_pipeline');
    } catch (err) {
        console.warn('cancelPipeline failed', err);
    }
}

export type StageSql = {
    node_id: string;
    label: string;
    kind: 'view' | 'sink';
    sql: string;
};

export async function compilePipelineSql(
    nodes: Node<DuckleNodeData>[],
    edges: Edge[],
): Promise<StageSql[] | null> {
    // null = compilation not available (web build / no Tauri). A real
    // compile failure THROWS the engine's error string so callers (the
    // Plan tab) can surface it; swallowing it here previously made the
    // Plan tab show a generic "appears here once it validates" placeholder
    // even when the pipeline had a clear planner error.
    if (!isTauri() && !isWebBackend()) return null;
    return await invoke<StageSql[]>('compile_pipeline', {
        pipeline: { nodes, edges },
    });
}

/** A resolved origin column for lineage (#103). */
export type LineageRoot = { node: string; column: string };
/** node id -> [output column name, root source columns][]. */
export type PipelineLineage = Record<string, Array<[string, LineageRoot[]]>>;

export async function pipelineColumnLineage(
    nodes: Node<DuckleNodeData>[],
    edges: Edge[],
): Promise<PipelineLineage | null> {
    if (!isTauri() && !isWebBackend()) return null;
    return await invoke<PipelineLineage>('pipeline_column_lineage', {
        pipeline: { nodes, edges },
    });
}

// ---- Schedules ---------------------------------------------------------

export type ScheduleKind =
    | { type: 'cron'; expr: string }
    | { type: 'interval'; seconds: number }
    | { type: 'file_watch'; path: string; recursive: boolean };

export type Schedule = {
    id: string;
    pipeline_id: string;
    name: string;
    enabled: boolean;
    kind: ScheduleKind;
    last_run_at?: string;
    last_run_status?: 'ok' | 'error' | 'cancelled';
    last_run_duration_ms?: number;
    last_run_error?: string;
    next_run_at?: string;
};

export async function scheduleSetWorkspace(path: string | null): Promise<void> {
    if (!isTauri()) return;
    try {
        await invoke('schedule_set_workspace', { path: path ?? '' });
    } catch (err) {
        console.warn('scheduleSetWorkspace failed', err);
    }
}

export async function scheduleList(): Promise<Schedule[]> {
    if (!isTauri()) return [];
    try {
        return await invoke<Schedule[]>('schedule_list');
    } catch (err) {
        console.warn('scheduleList failed', err);
        return [];
    }
}

export async function scheduleUpsert(schedule: Schedule): Promise<Schedule | null> {
    if (!isTauri()) return null;
    return await invoke<Schedule>('schedule_upsert', { schedule });
}

export async function scheduleDelete(id: string): Promise<void> {
    if (!isTauri()) return;
    await invoke('schedule_delete', { id });
}

export async function scheduleRunNow(id: string): Promise<RunResult | null> {
    if (!isTauri()) return null;
    return await invoke<RunResult>('schedule_run_now', { id });
}

// ---- App update check --------------------------------------------------

export type UpdateInfo = {
    update_available: boolean;
    current_build: string;
    latest_tag: string | null;
    latest_date: string | null;
    asset_name: string | null;
    release_url: string | null;
    download_url: string | null;
    error: string | null;
};

/**
 * Ask the backend whether a newer Duckle build is available on GitHub
 * (compares the running binary's build time to the latest release asset for
 * this OS). Returns null in browser mode or on any failure, so the banner
 * simply stays hidden when offline.
 */
export async function checkForUpdate(): Promise<UpdateInfo | null> {
    if (!isTauri()) return null;
    try {
        return await invoke<UpdateInfo>('check_for_update');
    } catch (err) {
        console.warn('checkForUpdate failed', err);
        return null;
    }
}

/** Progress phases streamed by the self_update backend command. */
export type SelfUpdateProgress =
    | { phase: 'downloading'; received: number; total?: number }
    | { phase: 'verifying' }
    | { phase: 'installing' }
    | { phase: 'ready' };

/**
 * Download + checksum-verify the latest release for this OS, swap it over the
 * running executable, and restart onto it - so the user never manually
 * downloads a build. On success the backend restarts the app (this promise may
 * not resolve because the process is replaced); on failure it rejects with a
 * message and the caller should fall back to the manual download link.
 */
export async function selfUpdate(onProgress?: (p: SelfUpdateProgress) => void): Promise<void> {
    const channel = new Channel<SelfUpdateProgress>();
    if (onProgress) channel.onmessage = onProgress;
    await invoke<void>('self_update', { onProgress: channel });
}

// ---- Build pipeline bundle ---------------------------------------------

export type SecretsMode = 'env' | 'passphrase';

/**
 * Build a single self-contained file for a pipeline via the embedded
 * duckle-runner. Returns the produced file path. Throws the runner's stderr
 * on failure so the caller can show it inline.
 */
export type TargetOs = 'windows' | 'linux' | 'macos';

export type BuildCapabilities = {
    hostOs: TargetOs;
    canTargetLinux: boolean;
};

/**
 * What target OSes this build of Duckle can actually produce. Used so the Build
 * Pipeline dialog never offers a target it cannot build (e.g. a Linux artifact
 * when this build did not bundle the Linux runner).
 */
export async function buildCapabilities(): Promise<BuildCapabilities> {
    return await invoke<BuildCapabilities>('build_capabilities');
}

export async function buildBundle(
    workspacePath: string,
    pipelineId: string,
    outFile: string,
    context: string | null,
    secretsMode: SecretsMode,
    passphrase?: string,
    targetOs?: TargetOs,
): Promise<string> {
    return await invoke<string>('build_pipeline_bundle', {
        workspacePath,
        pipelineId,
        outFile,
        context: context ?? null,
        secretsMode,
        passphrase: secretsMode === 'passphrase' ? (passphrase ?? '') : null,
        targetOs: targetOs ?? null,
    });
}

// ---- MCP server ---------------------------------------------------------

export type McpConnInfo = {
    bundled: boolean;
    duckdbFound: boolean;
    claudeCli: boolean;
    mcpPath: string;
    duckdbPath: string;
    runnerPath: string;
    claudeCommand: string;
    configJson: string;
};

/**
 * Resolve the bundled MCP server: stages it to app-data and returns the
 * paths plus a ready-to-paste `claude mcp add` command and mcpServers JSON.
 */
export async function mcpConnectionInfo(): Promise<McpConnInfo> {
    return await invoke<McpConnInfo>('mcp_connection_info');
}

/**
 * Run `claude mcp add duckle ...` to connect Claude Code in one click.
 * Resolves with the CLI output; rejects (so the caller can show it) when the
 * CLI is missing or the add fails.
 */
export async function connectClaudeCode(): Promise<string> {
    return await invoke<string>('connect_claude_code');
}

export type McpClient = 'claude_desktop' | 'cursor';

/**
 * Inject the duckle MCP server into a desktop client's config file (Claude
 * Desktop or Cursor), merging into any existing mcpServers. Resolves with the
 * written config path; rejects (with a hint) when the write needs permissions
 * or the existing file is not valid JSON.
 */
export async function mcpInjectConfig(client: McpClient): Promise<string> {
    return await invoke<string>('mcp_inject_config', { client });
}

/**
 * Read the workspace's saved HTTP/HTTPS proxy (issue #80). Null = direct.
 */
export async function settingsGetProxy(workspace: string): Promise<string | null> {
    return (await invoke<string | null>('settings_get_proxy', { workspace })) ?? null;
}

/**
 * Persist and immediately apply the workspace's HTTP/HTTPS proxy (no system env
 * var needed). Pass null to clear. Routes REST / cloud connectors and the
 * in-app updater through the proxy.
 */
export async function settingsSetProxy(workspace: string, url: string | null): Promise<void> {
    await invoke('settings_set_proxy', { workspace, url });
}

// ---- Per-workspace memory cap (#102) -----------------------------------

/** Read the workspace total DuckDB memory cap in MB (null = engine default). */
export async function settingsGetMemoryLimit(workspace: string): Promise<number | null> {
    if (!workspace) return null;
    try {
        return (await invoke<number | null>('settings_get_memory_limit', { workspace })) ?? null;
    } catch {
        return null;
    }
}

/**
 * Persist and immediately apply the workspace total memory cap (MB), used as
 * DUCKLE_MEMORY_LIMIT for every run (batched and per-stage). Pass null to clear.
 */
export async function settingsSetMemoryLimit(workspace: string, mb: number | null): Promise<void> {
    await invoke('settings_set_memory_limit', { workspace, mb });
}

// ---- Global context file (key/value file -> global ${context}) ---------

/** Read the configured global-context file path (null = none). */
export async function settingsGetContextFile(workspace: string): Promise<string | null> {
    if (!workspace) return null;
    try {
        return (await invoke<string | null>('settings_get_context_file', { workspace })) ?? null;
    } catch {
        return null;
    }
}

/** Persist the global-context file path. Pass null to clear. */
export async function settingsSetContextFile(workspace: string, path: string | null): Promise<void> {
    await invoke('settings_set_context_file', { workspace, path });
}

/**
 * Resolve the global-context file into a flat var map for the desktop run path
 * (the headless runner / web server resolve it engine-side). Empty on any error
 * or when no file is configured.
 */
export async function settingsLoadContextVars(workspace: string): Promise<Record<string, string>> {
    if (!workspace) return {};
    try {
        return (await invoke<Record<string, string>>('settings_load_context_vars', { workspace })) ?? {};
    } catch {
        return {};
    }
}

// ---- External AI endpoint for the Duckie assistant (#92) ----------------

export type AiConfig = { baseUrl: string | null; model: string | null; apiKey: string | null };

/** Read the workspace's external OpenAI-compatible AI config (empty = local Qwen). */
export async function settingsGetAi(workspace: string): Promise<AiConfig> {
    if (!isTauri() || !workspace) return { baseUrl: null, model: null, apiKey: null };
    try {
        return await invoke<AiConfig>('settings_get_ai', { workspace });
    } catch {
        return { baseUrl: null, model: null, apiKey: null };
    }
}

/** Persist the external AI endpoint. Empty baseUrl reverts to the local model. */
export async function settingsSetAi(
    workspace: string,
    cfg: { baseUrl: string | null; model: string | null; apiKey: string | null },
): Promise<void> {
    await invoke('settings_set_ai', {
        workspace,
        baseUrl: cfg.baseUrl,
        model: cfg.model,
        apiKey: cfg.apiKey,
    });
}
