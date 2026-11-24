// The swarm view's state, folded from the events `duckle-mcp swarm` prints.
//
// Pure: the panel keeps one SwarmState and replaces it with
// applySwarmEvent(state, event) for every event, so what the view shows is a
// function of the events alone (and checks/check-logic.ts can replay them).

export type RoleId = 'scout' | 'architect' | 'builder' | 'tester' | 'fixer' | 'reviewer';

export const ROLES: { id: RoleId; title: string; job: string }[] = [
    { id: 'scout', title: 'Scout', job: 'reads the data' },
    { id: 'architect', title: 'Architect', job: 'designs the pipeline and its gates' },
    { id: 'builder', title: 'Builder', job: 'writes it onto the canvas' },
    { id: 'tester', title: 'Tester', job: 'runs it and checks every gate' },
    { id: 'fixer', title: 'Fixer', job: 'repairs what failed' },
    { id: 'reviewer', title: 'Reviewer', job: 'reports back to you' },
];

export type SwarmEvent =
    | { type: 'started'; goal: string; pipelineId: string; model: string; roles: RoleId[] }
    | {
          type: 'agent';
          role: RoleId;
          state: 'working' | 'done' | 'failed';
          summary?: string;
          verdict?: 'pass' | 'fail' | null;
      }
    | { type: 'text' | 'thinking'; role: RoleId; text: string }
    | { type: 'retry'; role: RoleId; reason: string }
    | { type: 'tool'; role: RoleId; id: string; name: string; input: unknown }
    | { type: 'toolResult'; role: RoleId; id: string; name: string; ok: boolean; ms: number; preview: string }
    | { type: 'pipeline'; id: string; name: string; doc: { nodes?: unknown[]; edges?: unknown[] } & Record<string, unknown> }
    | { type: 'run'; status: string; durationMs?: number; nodes?: Record<string, { status?: string; rows?: number; error?: string }>; error?: unknown }
    | { type: 'usage'; role: RoleId; inputTokens: number; outputTokens: number }
    | { type: 'handoff'; from: RoleId; to: RoleId }
    | {
          type: 'finished';
          ok: boolean;
          tested?: boolean;
          stopped?: boolean;
          summary: string;
          pipelineId?: string;
          durationMs?: number;
          toolCalls?: number;
          inputTokens?: number;
          outputTokens?: number;
      }
    | { type: 'error'; message: string };

export type AgentState = 'idle' | 'working' | 'done' | 'failed';

export type ToolChip = {
    id: string;
    name: string;
    /** A few words on what the call was about: a path, a component id. */
    about: string;
    state: 'running' | 'ok' | 'error';
    ms?: number;
    preview?: string;
};

/** One agent's turn on stage. The Tester and Fixer can each have several. */
export type Stint = {
    role: RoleId;
    state: AgentState;
    tools: ToolChip[];
    summary?: string;
    verdict?: 'pass' | 'fail' | null;
};

export type SwarmState = {
    phase: 'idle' | 'running' | 'finished';
    goal: string;
    model: string;
    pipelineId: string | null;
    agents: Record<RoleId, AgentState>;
    active: RoleId | null;
    stints: Stint[];
    /** What the active agent is saying right now, thinking marked apart. */
    stream: { role: RoleId; thinking: string; text: string } | null;
    toolCalls: number;
    inputTokens: number;
    outputTokens: number;
    nodeCount: number;
    gateCount: number;
    rows: number | null;
    runs: number;
    lastRun: 'ok' | 'error' | null;
    error: string | null;
    finished: Extract<SwarmEvent, { type: 'finished' }> | null;
};

const IDLE_AGENTS: Record<RoleId, AgentState> = {
    scout: 'idle',
    architect: 'idle',
    builder: 'idle',
    tester: 'idle',
    fixer: 'idle',
    reviewer: 'idle',
};

export function initialSwarmState(): SwarmState {
    return {
        phase: 'idle',
        goal: '',
        model: '',
        pipelineId: null,
        agents: { ...IDLE_AGENTS },
        active: null,
        stints: [],
        stream: null,
        toolCalls: 0,
        inputTokens: 0,
        outputTokens: 0,
        nodeCount: 0,
        gateCount: 0,
        rows: null,
        runs: 0,
        lastRun: null,
        error: null,
        finished: null,
    };
}

/** The stream keeps the tail an eye can follow, not the whole turn. */
const STREAM_KEEP = 900;

function tail(s: string): string {
    return s.length > STREAM_KEEP ? s.slice(s.length - STREAM_KEEP) : s;
}

/** A few words on what a tool call was about, for its chip. */
export function aboutCall(name: string, input: unknown): string {
    const a = (input && typeof input === 'object' ? input : {}) as Record<string, unknown>;
    const str = (k: string) => (typeof a[k] === 'string' ? (a[k] as string) : '');
    const short = (p: string) => p.replace(/^\$\{workspace\}[\\/]/, '').split(/[\\/]/).slice(-2).join('/');
    switch (name) {
        case 'inspect_source':
            return short(str('path'));
        case 'get_component_schema':
            return str('componentId');
        case 'list_components':
            return [str('kind'), str('query')].filter(Boolean).join(' ') || 'all';
        case 'create_pipeline': {
            const p = a.pipeline as { nodes?: unknown[] } | undefined;
            const n = Array.isArray(p?.nodes) ? p.nodes.length : 0;
            return n ? `${n} node${n === 1 ? '' : 's'}` : str('name');
        }
        case 'update_pipeline': {
            const p = a.patch as { nodes?: unknown[]; edges?: unknown[] } | undefined;
            const n = (Array.isArray(p?.nodes) ? p.nodes.length : 0) + (Array.isArray(p?.edges) ? p.edges.length : 0);
            return n ? `patch ${n}` : 'patch';
        }
        case 'run_pipeline':
            return str('target') ? `to ${str('target')}` : 'full run';
        case 'check_node_sql':
            return str('node');
        default:
            return '';
    }
}

/** Quality gates in a pipeline document: its qa.* steps. */
function countGates(nodes: unknown[]): number {
    return nodes.filter(n => {
        const cid = (n as { data?: { componentId?: unknown } })?.data?.componentId;
        return typeof cid === 'string' && cid.startsWith('qa.');
    }).length;
}

export function applySwarmEvent(s: SwarmState, e: SwarmEvent): SwarmState {
    switch (e.type) {
        case 'started':
            return {
                ...initialSwarmState(),
                phase: 'running',
                goal: e.goal,
                model: e.model,
                pipelineId: e.pipelineId,
            };
        case 'agent': {
            const agents = { ...s.agents, [e.role]: e.state };
            if (e.state === 'working') {
                return {
                    ...s,
                    agents,
                    active: e.role,
                    stream: { role: e.role, thinking: '', text: '' },
                    stints: [...s.stints, { role: e.role, state: 'working', tools: [] }],
                };
            }
            const stints = s.stints.slice();
            for (let i = stints.length - 1; i >= 0; i--) {
                if (stints[i].role === e.role && stints[i].state === 'working') {
                    stints[i] = { ...stints[i], state: e.state, summary: e.summary, verdict: e.verdict ?? null };
                    break;
                }
            }
            return { ...s, agents, stints, active: s.active === e.role ? null : s.active };
        }
        case 'text':
        case 'thinking': {
            const cur = s.stream && s.stream.role === e.role ? s.stream : { role: e.role, thinking: '', text: '' };
            return {
                ...s,
                stream:
                    e.type === 'text'
                        ? { ...cur, text: tail(cur.text + e.text) }
                        : { ...cur, thinking: tail(cur.thinking + e.text) },
            };
        }
        case 'retry':
            // The turn is being asked for again: what arrived of it is void.
            return { ...s, stream: { role: e.role, thinking: '', text: '' } };
        case 'tool': {
            const chip: ToolChip = { id: e.id, name: e.name, about: aboutCall(e.name, e.input), state: 'running' };
            return { ...s, toolCalls: s.toolCalls + 1, stints: withLastStint(s.stints, e.role, st => ({ ...st, tools: [...st.tools, chip] })) };
        }
        case 'toolResult':
            return {
                ...s,
                stints: withLastStint(s.stints, e.role, st => ({
                    ...st,
                    tools: st.tools.map(t =>
                        t.id === e.id ? { ...t, state: e.ok ? 'ok' : 'error', ms: e.ms, preview: e.preview } : t,
                    ),
                })),
            };
        case 'pipeline': {
            const nodes = Array.isArray(e.doc?.nodes) ? e.doc.nodes : [];
            return { ...s, nodeCount: nodes.length, gateCount: countGates(nodes) };
        }
        case 'run': {
            const ok = e.status === 'ok';
            let rows: number | null = s.rows;
            if (ok && e.nodes) {
                // The largest row count any step reported: the run's scale.
                const counts = Object.values(e.nodes)
                    .map(n => n?.rows)
                    .filter((r): r is number => typeof r === 'number');
                if (counts.length) rows = Math.max(...counts);
            }
            return { ...s, runs: s.runs + 1, lastRun: ok ? 'ok' : 'error', rows };
        }
        case 'usage':
            return { ...s, inputTokens: e.inputTokens, outputTokens: e.outputTokens };
        case 'handoff':
            return s;
        case 'finished':
            return { ...s, phase: 'finished', active: null, stream: null, finished: e };
        case 'error':
            return { ...s, error: e.message };
    }
}

function withLastStint(stints: Stint[], role: RoleId, f: (s: Stint) => Stint): Stint[] {
    for (let i = stints.length - 1; i >= 0; i--) {
        if (stints[i].role === role) {
            const next = stints.slice();
            next[i] = f(stints[i]);
            return next;
        }
    }
    return stints;
}

/** "12.4k" for a token count. */
export function compact(n: number): string {
    if (n < 1000) return String(n);
    if (n < 1_000_000) return `${(n / 1000).toFixed(n < 10_000 ? 1 : 0)}k`;
    return `${(n / 1_000_000).toFixed(1)}M`;
}
