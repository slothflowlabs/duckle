// The swarm view's state, folded from the events `duckle-mcp swarm` prints,
// and the crew the view sends with a run.
//
// Pure: the panel keeps one SwarmState and replaces it with
// applySwarmEvent(state, event) for every event, so what the view shows is a
// function of the events alone (and scripts/check-logic.ts can replay them).

export type StageId = 'discover' | 'design' | 'build' | 'test' | 'repair' | 'review';

export const STAGES: { id: StageId; title: string; job: string; sideBySide: boolean }[] = [
    { id: 'discover', title: 'Discover', job: 'reads the data', sideBySide: true },
    { id: 'design', title: 'Design', job: 'plans the pipeline and its gates', sideBySide: false },
    { id: 'build', title: 'Build', job: 'writes it onto the canvas', sideBySide: false },
    { id: 'test', title: 'Test', job: 'runs it and checks every gate', sideBySide: false },
    { id: 'repair', title: 'Repair', job: 'fixes what the tests found', sideBySide: false },
    { id: 'review', title: 'Review', job: 'reports back to you', sideBySide: true },
];

/** One member of the crew, as the view keeps it and the run receives it. */
export type CrewAgent = {
    id: string;
    name: string;
    stage: StageId;
    /** Its own instructions, on top of its stage's. Empty for a built-in. */
    brief: string;
    builtIn?: boolean;
};

export const DEFAULT_CREW: CrewAgent[] = [
    { id: 'scout', name: 'Scout', stage: 'discover', brief: '', builtIn: true },
    { id: 'architect', name: 'Architect', stage: 'design', brief: '', builtIn: true },
    { id: 'builder', name: 'Builder', stage: 'build', brief: '', builtIn: true },
    { id: 'tester', name: 'Tester', stage: 'test', brief: '', builtIn: true },
    { id: 'fixer', name: 'Fixer', stage: 'repair', brief: '', builtIn: true },
    { id: 'reviewer', name: 'Reviewer', stage: 'review', brief: '', builtIn: true },
];

/** Specialists one click adds to the crew. */
export const TEMPLATES: Omit<CrewAgent, 'id'>[] = [
    {
        name: 'Data Profiler',
        stage: 'discover',
        brief: 'Profile every column the goal touches: nulls, distinct counts, ranges, and values that look wrong.',
    },
    {
        name: 'Join Scout',
        stage: 'discover',
        brief: 'Find how the files relate: candidate join keys, their match rate, and keys present on one side only.',
    },
    {
        name: 'Edge-case Tester',
        stage: 'test',
        brief: 'Try to break it: null keys, duplicates, empty input, extreme values. Run with target to inspect steps.',
    },
    {
        name: 'PII Guardian',
        stage: 'review',
        brief: 'Find columns that hold personal data. Fail if raw personal data reaches a sink unmasked.',
    },
    {
        name: 'Performance Critic',
        stage: 'review',
        brief: 'Look for expensive steps: reading a file twice, wide sorts, joins that multiply rows. Suggest cheaper shapes.',
    },
    {
        name: 'Docs Writer',
        stage: 'review',
        brief: 'Write a short README for the pipeline in your summary: purpose, inputs, outputs, gates, how to rerun it.',
    },
];

/** A crew the run accepts: one Build agent at least, ids unique. */
export function crewProblem(crew: CrewAgent[]): string | null {
    if (!crew.some(a => a.stage === 'build')) return 'The crew needs a Build agent: nobody else writes the pipeline.';
    const ids = new Set<string>();
    for (const a of crew) {
        if (!a.name.trim()) return 'Every agent needs a name.';
        if (ids.has(a.id)) return `Two agents share the id ${a.id}.`;
        ids.add(a.id);
    }
    return null;
}

/** A fresh id for a new agent, never one the crew has. */
export function newAgentId(crew: CrewAgent[], name: string): string {
    const base = name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '').slice(0, 30) || 'agent';
    let id = base;
    for (let n = 2; crew.some(a => a.id === id); n++) id = `${base}-${n}`;
    return id;
}

export type SwarmEvent =
    | {
          type: 'started';
          goal: string;
          pipelineId: string;
          model: string;
          agents: { id: string; name: string; stage: StageId }[];
      }
    | { type: 'stage'; stage: StageId; agents: string[] }
    | {
          type: 'agent';
          agent: string;
          state: 'working' | 'done' | 'failed';
          summary?: string;
          verdict?: 'pass' | 'fail' | null;
      }
    | { type: 'text' | 'thinking'; agent: string; text: string }
    | { type: 'retry'; agent: string; reason: string }
    | { type: 'tool'; agent: string; id: string; name: string; input: unknown }
    | { type: 'toolResult'; agent: string; id: string; name: string; ok: boolean; ms: number; preview: string }
    | { type: 'pipeline'; id: string; name: string; doc: { nodes?: unknown[]; edges?: unknown[] } & Record<string, unknown> }
    | {
          type: 'run';
          status: string;
          durationMs?: number;
          nodes?: Record<string, { status?: string; rows?: number; error?: string }>;
          error?: unknown;
      }
    | { type: 'usage'; agent: string; inputTokens: number; outputTokens: number }
    | { type: 'handoff'; from: string; to: string }
    | {
          type: 'finished';
          ok: boolean;
          tested?: boolean;
          reviewed?: boolean;
          stopped?: boolean;
          summary: string;
          pipelineId?: string;
          agents?: number;
          durationMs?: number;
          toolCalls?: number;
          inputTokens?: number;
          outputTokens?: number;
      }
    | { type: 'error'; message: string };

export type AgentState = 'idle' | 'working' | 'done' | 'failed';
export type StageState = 'pending' | 'active' | 'done' | 'failed';

export type ToolChip = {
    id: string;
    name: string;
    /** A few words on what the call was about: a path, a component id. */
    about: string;
    state: 'running' | 'ok' | 'error';
    ms?: number;
    preview?: string;
};

/** One agent's turn on stage. Test and Repair agents can have several. */
export type Stint = {
    agent: string;
    state: AgentState;
    tools: ToolChip[];
    /** What it is saying right now, thinking kept apart. Cleared when done. */
    thinking: string;
    text: string;
    summary?: string;
    verdict?: 'pass' | 'fail' | null;
};

export type SwarmState = {
    phase: 'idle' | 'running' | 'finished';
    goal: string;
    model: string;
    pipelineId: string | null;
    /** The crew of this run, from its started event. */
    crew: { id: string; name: string; stage: StageId }[];
    agents: Record<string, AgentState>;
    stages: Record<StageId, StageState>;
    stage: StageId | null;
    stints: Stint[];
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

const PENDING_STAGES: Record<StageId, StageState> = {
    discover: 'pending',
    design: 'pending',
    build: 'pending',
    test: 'pending',
    repair: 'pending',
    review: 'pending',
};

export function initialSwarmState(): SwarmState {
    return {
        phase: 'idle',
        goal: '',
        model: '',
        pipelineId: null,
        crew: [],
        agents: {},
        stages: { ...PENDING_STAGES },
        stage: null,
        stints: [],
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

/** The agents working right now. */
export function working(s: SwarmState): string[] {
    return Object.entries(s.agents)
        .filter(([, st]) => st === 'working')
        .map(([id]) => id);
}

/** A stream keeps the tail an eye can follow, not the whole turn. */
const STREAM_KEEP = 700;

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

/** The agent's open stint, else its latest. */
function stintIndex(stints: Stint[], agent: string): number {
    for (let i = stints.length - 1; i >= 0; i--) if (stints[i].agent === agent && stints[i].state === 'working') return i;
    for (let i = stints.length - 1; i >= 0; i--) if (stints[i].agent === agent) return i;
    return -1;
}

function withStint(stints: Stint[], agent: string, f: (s: Stint) => Stint): Stint[] {
    const i = stintIndex(stints, agent);
    if (i < 0) return stints;
    const next = stints.slice();
    next[i] = f(stints[i]);
    return next;
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
                crew: e.agents ?? [],
                agents: Object.fromEntries((e.agents ?? []).map(a => [a.id, 'idle' as AgentState])),
            };
        case 'stage': {
            // A stage that starts again (Test after a repair) is active again;
            // the one before it is done.
            const stages = { ...s.stages };
            if (s.stage && s.stage !== e.stage && stages[s.stage] === 'active') stages[s.stage] = 'done';
            stages[e.stage] = 'active';
            return { ...s, stage: e.stage, stages };
        }
        case 'agent': {
            const agents = { ...s.agents, [e.agent]: e.state };
            if (e.state === 'working') {
                return {
                    ...s,
                    agents,
                    stints: [...s.stints, { agent: e.agent, state: 'working', tools: [], thinking: '', text: '' }],
                };
            }
            const stints = withStint(s.stints, e.agent, st => ({
                ...st,
                state: e.state,
                summary: e.summary,
                verdict: e.verdict ?? null,
                thinking: '',
                text: '',
            }));
            const stages = { ...s.stages };
            const stage = s.crew.find(a => a.id === e.agent)?.stage;
            if (stage && e.state === 'failed') stages[stage] = 'failed';
            return { ...s, agents, stints, stages };
        }
        case 'text':
        case 'thinking':
            return {
                ...s,
                stints: withStint(s.stints, e.agent, st =>
                    e.type === 'text' ? { ...st, text: tail(st.text + e.text) } : { ...st, thinking: tail(st.thinking + e.text) },
                ),
            };
        case 'retry':
            // The turn is being asked for again: what arrived of it is void.
            return { ...s, stints: withStint(s.stints, e.agent, st => ({ ...st, thinking: '', text: '' })) };
        case 'tool': {
            const chip: ToolChip = { id: e.id, name: e.name, about: aboutCall(e.name, e.input), state: 'running' };
            return {
                ...s,
                toolCalls: s.toolCalls + 1,
                stints: withStint(s.stints, e.agent, st => ({ ...st, tools: [...st.tools, chip] })),
            };
        }
        case 'toolResult':
            return {
                ...s,
                stints: withStint(s.stints, e.agent, st => ({
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
        case 'finished': {
            const stages = { ...s.stages };
            if (s.stage && stages[s.stage] === 'active') stages[s.stage] = e.ok || !e.stopped ? 'done' : 'failed';
            // A swarm that died mid-turn sent no agent event: once it is over, nobody is working.
            const settled = e.ok ? ('done' as const) : ('failed' as const);
            const agents = Object.fromEntries(
                Object.entries(s.agents).map(([id, st]) => [id, st === 'working' ? settled : st]),
            );
            const stints = s.stints.map(t => (t.state === 'working' ? { ...t, state: settled } : t));
            return { ...s, phase: 'finished', stages, agents, stints, finished: e };
        }
        case 'error':
            return { ...s, error: e.message };
    }
}

/** "12.4k" for a token count. */
export function compact(n: number): string {
    if (n < 1000) return String(n);
    if (n < 1_000_000) return `${(n / 1000).toFixed(n < 10_000 ? 1 : 0)}k`;
    return `${(n / 1_000_000).toFixed(1)}M`;
}
