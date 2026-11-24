// Swarm mode: describe a mission, pick a crew of any size and a model, and
// watch the agents build, run and test the pipeline in front of you. The
// pipeline appears on the canvas as it is written; each run lights up its
// steps with row counts.
//
// Four tabs keep the panel short: Mission (the goal), Crew (the agents, any
// number, by stage), Model (where the model runs) and Live (the run).

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
    Cloud,
    CircleStop,
    Cpu,
    KeyRound,
    Plus,
    RotateCcw,
    Rocket,
    Sparkles,
    Trash2,
    UserRound,
    X,
} from 'lucide-react';
import SwarmConstellation, { type Flight } from './SwarmConstellation';
import { STAGE_COLOR, STAGE_ICON } from './swarm-look';
import {
    DEFAULT_CREW,
    STAGES,
    TEMPLATES,
    applySwarmEvent,
    compact,
    crewProblem,
    initialSwarmState,
    newAgentId,
    working,
    type CrewAgent,
    type StageId,
    type SwarmEvent,
    type SwarmState,
} from './swarm-state';
import { swarmRun, swarmStatus, swarmStop, type SwarmProvider, type SwarmStatus } from './swarm-api';
import { isWebBackend } from '../web-fs';
import './swarm.css';

type Tab = 'mission' | 'crew' | 'model' | 'live';

type Source =
    | 'claude-account'
    | 'claude-key'
    | 'huggingface'
    | 'openai'
    | 'openrouter'
    | 'ollama'
    | 'lmstudio'
    | 'custom';

type SourceDef = {
    id: Source;
    label: string;
    sub: string;
    base?: string;
    keyEnv?: string;
    local?: boolean;
    models: string[];
    hint: string;
};

const CLAUDE_MODELS = ['claude-opus-5-5', 'claude-sonnet-5-5', 'claude-haiku-5-5', 'claude-fable-5-2'];

const SOURCES: SourceDef[] = [
    { id: 'claude-account', label: 'Claude', sub: 'your Anthropic account', models: CLAUDE_MODELS, hint: 'claude-opus-5-5' },
    { id: 'claude-key', label: 'Claude', sub: 'API key', keyEnv: 'ANTHROPIC_API_KEY', models: CLAUDE_MODELS, hint: 'claude-opus-5-5' },
    {
        id: 'huggingface',
        label: 'Hugging Face',
        sub: 'open models, your HF token',
        base: 'https://router.huggingface.co/v1',
        keyEnv: 'HF_TOKEN',
        models: ['zai-org/GLM-5.3', 'deepseek-ai/DeepSeek-V4.1-Flash-1017', 'moonshotai/Kimi-K3.1-Pro', 'openai/gpt-oss-120b', 'Qwen/Qwen3.8-27B'],
        hint: 'zai-org/GLM-5.3',
    },
    { id: 'openai', label: 'OpenAI', sub: 'API key', base: 'https://api.openai.com/v1', keyEnv: 'OPENAI_API_KEY', models: [], hint: 'a model id from your OpenAI account' },
    {
        id: 'openrouter',
        label: 'OpenRouter',
        sub: 'any model, one key',
        base: 'https://openrouter.ai/api/v1',
        keyEnv: 'OPENROUTER_API_KEY',
        models: [],
        hint: 'a model id from openrouter.ai/models',
    },
    { id: 'ollama', label: 'Ollama', sub: 'on this machine', base: 'http://localhost:11434/v1', local: true, models: [], hint: 'a pulled model with tool calling' },
    { id: 'lmstudio', label: 'LM Studio', sub: 'on this machine', base: 'http://localhost:1234/v1', local: true, models: [], hint: 'the loaded model' },
    { id: 'custom', label: 'Custom', sub: 'any OpenAI-compatible endpoint', models: [], hint: 'model id' },
];

const EXAMPLES = [
    'Revenue per customer from the orders file, newest year only, to Parquet.',
    'One clean row per customer email, names trimmed, countries upper case.',
    'Units and revenue per category per month, orders joined to products.',
];

type Prefs = { source: Source; models: Partial<Record<Source, string>>; baseUrl: string };

const PREFS_KEY = 'duckle.swarm.model.v2';
const CREW_KEY = 'duckle.swarm.crew.v1';

function load<T>(key: string, fallback: T, ok: (v: unknown) => boolean): T {
    try {
        const raw = localStorage.getItem(key);
        if (!raw) return fallback;
        const v = JSON.parse(raw) as unknown;
        return ok(v) ? (v as T) : fallback;
    } catch {
        return fallback;
    }
}

function save(key: string, v: unknown): void {
    try {
        localStorage.setItem(key, JSON.stringify(v));
    } catch {
        // A private window: the choice lasts this session.
    }
}

const STAGE_IDS = STAGES.map(s => s.id);

function isCrew(v: unknown): boolean {
    return (
        Array.isArray(v) &&
        v.every(
            a =>
                a &&
                typeof a === 'object' &&
                typeof (a as CrewAgent).id === 'string' &&
                typeof (a as CrewAgent).name === 'string' &&
                STAGE_IDS.includes((a as CrewAgent).stage),
        )
    );
}

function clock(ms: number): string {
    const s = Math.floor(ms / 1000);
    return `${String(Math.floor(s / 60)).padStart(2, '0')}:${String(s % 60).padStart(2, '0')}`;
}

export type SwarmRunNodes = Record<string, { status?: string; rows?: number; error?: string }>;

export default function SwarmPanel({
    workspacePath,
    onClose,
    onPipeline,
    onRun,
    onLive,
}: {
    workspacePath: string | null;
    onClose: () => void;
    /** The Build or Repair stage wrote the pipeline: show it on the canvas. */
    onPipeline: (id: string, name: string, doc: Record<string, unknown>) => void;
    /** A Test agent ran it: show each step's result on the canvas. */
    onRun: (id: string, status: string, durationMs: number, nodes: SwarmRunNodes) => void;
    /** Whether a swarm is running, for the canvas's live styling. */
    onLive: (live: boolean) => void;
}) {
    const [state, setState] = useState<SwarmState>(initialSwarmState);
    const [tab, setTab] = useState<Tab>('mission');
    const [goal, setGoal] = useState('');
    const [crew, setCrew] = useState<CrewAgent[]>(() => load(CREW_KEY, DEFAULT_CREW, isCrew));
    const [prefs, setPrefs] = useState<Prefs>(() =>
        load(PREFS_KEY, { source: 'claude-account', models: {}, baseUrl: '' }, v => !!v && typeof v === 'object' && 'source' in (v as object)),
    );
    const [apiKey, setApiKey] = useState('');
    const [status, setStatus] = useState<SwarmStatus | null>(null);
    const [flights, setFlights] = useState<Flight[]>([]);
    const [busyTool, setBusyTool] = useState<string | null>(null);
    const [startedAt, setStartedAt] = useState<number | null>(null);
    const [, setTick] = useState(0);
    const abort = useRef<AbortController | null>(null);
    const pipelineId = useRef<string | null>(null);

    const running = state.phase === 'running';
    const source = SOURCES.find(s => s.id === prefs.source) ?? SOURCES[0];
    const model = (prefs.models[prefs.source] ?? '').trim();

    useEffect(() => {
        void swarmStatus().then(setStatus);
    }, []);
    useEffect(() => save(PREFS_KEY, prefs), [prefs]);
    useEffect(() => save(CREW_KEY, crew), [crew]);
    useEffect(() => {
        if (!running) return;
        const t = window.setInterval(() => setTick(n => n + 1), 1000);
        return () => window.clearInterval(t);
    }, [running]);
    useEffect(() => {
        onLive(running);
        return () => onLive(false);
    }, [running, onLive]);

    const provider = useMemo((): SwarmProvider => {
        if (prefs.source === 'claude-account') return { kind: 'anthropic-account', model: model || undefined };
        if (prefs.source === 'claude-key') return { kind: 'anthropic-key', model: model || undefined, apiKey: apiKey || undefined };
        const baseUrl = prefs.source === 'custom' ? prefs.baseUrl.trim() : source.base;
        return { kind: 'openai-compatible', model: model || source.hint, baseUrl, apiKey: apiKey || undefined };
    }, [prefs, model, apiKey, source]);

    /** Why Launch cannot go yet, if it cannot. */
    const blocker = useMemo((): string | null => {
        if (!goal.trim()) return 'Describe the mission first.';
        if (!workspacePath && !isWebBackend()) return 'Open a workspace first.';
        const crewIssue = crewProblem(crew);
        if (crewIssue) return crewIssue;
        if (prefs.source === 'claude-account' && status && !status.anthropicAccount.installed)
            return "Install Anthropic's CLI (ant) to use your account, or choose an API key.";
        const needsModel = !['claude-account', 'claude-key'].includes(prefs.source) && !source.models.length;
        if (needsModel && !model) return 'Name the model to use (Model tab).';
        if (prefs.source === 'custom' && !prefs.baseUrl.trim()) return "Give the endpoint's base URL (Model tab).";
        return null;
    }, [goal, workspacePath, crew, prefs, status, source, model]);

    const handle = useCallback(
        (e: SwarmEvent) => {
            setState(s => applySwarmEvent(s, e));
            switch (e.type) {
                case 'started':
                    pipelineId.current = e.pipelineId;
                    break;
                case 'tool':
                    setBusyTool(e.name);
                    setFlights(f => [...f, { kind: 'call', agent: e.agent }]);
                    break;
                case 'toolResult':
                    setBusyTool(null);
                    setFlights(f => [...f, { kind: 'answer', agent: e.agent, ok: e.ok }]);
                    break;
                case 'handoff':
                    setFlights(f => [...f, { kind: 'handoff', from: e.from, to: e.to }]);
                    break;
                case 'pipeline':
                    onPipeline(e.id, e.name, e.doc as Record<string, unknown>);
                    break;
                case 'run':
                    if (pipelineId.current && e.nodes) onRun(pipelineId.current, e.status, e.durationMs ?? 0, e.nodes);
                    break;
                case 'finished':
                    setBusyTool(null);
                    break;
            }
        },
        [onPipeline, onRun],
    );

    const launch = useCallback(async () => {
        if (blocker || running) return;
        setFlights([]);
        setState({ ...initialSwarmState(), phase: 'running', goal: goal.trim() });
        setStartedAt(Date.now());
        setTab('live');
        const ctl = new AbortController();
        abort.current = ctl;
        await swarmRun(
            {
                goal: goal.trim(),
                workspace: workspacePath ?? '',
                provider,
                agents: crew.map(({ id, name, stage, brief }) => ({ id, name, stage, brief })),
            },
            handle,
            ctl.signal,
        );
        abort.current = null;
        // A run that ended without its finished event (a transport failure)
        // still has to leave the running state.
        setState(s => (s.phase === 'running' ? { ...s, phase: 'finished' } : s));
    }, [blocker, running, goal, workspacePath, provider, crew, handle]);

    const stop = useCallback(() => {
        abort.current?.abort();
        void swarmStop();
    }, []);

    const fresh = useCallback(() => {
        setState(initialSwarmState());
        setStartedAt(null);
        setFlights([]);
        setTab('mission');
    }, []);

    const elapsed = startedAt ? (state.finished?.durationMs ?? Date.now() - startedAt) : 0;
    const shownCrew = state.phase === 'idle' ? crew : state.crew;
    const busy = working(state);
    const nameOf = (id: string) => shownCrew.find(a => a.id === id)?.name ?? id;
    const finished = state.finished;
    const outcome = finished && !finished.stopped ? (finished.ok ? 'pass' : 'fail') : null;
    const activeStage = STAGES.find(s => s.id === state.stage);

    return (
        <aside className={`swarm-panel${running ? ' is-running' : ''}`} aria-label="Agent swarm">
            <header className="sw-head">
                <span className="sw-mark" aria-hidden="true" />
                <div className="sw-titles">
                    <span className="sw-word">SWARM</span>
                    <span className="sw-sub">
                        {shownCrew.length} agents &middot; {STAGES.length} stages &middot; one tested pipeline
                    </span>
                </div>
                <button type="button" className="sw-x" onClick={onClose} aria-label="Close the swarm" title="Close">
                    <X size={15} />
                </button>
            </header>

            <div className="sw-hero">
                <SwarmConstellation crew={shownCrew} agents={state.agents} busyTool={busyTool} flights={flights} />
                <div className="sw-status">
                    {running ? (
                        <>
                            <span className="sw-pulse" style={{ background: activeStage ? STAGE_COLOR[activeStage.id] : 'var(--accent)' }} />
                            <span className="sw-status-text">
                                {busy.length ? <b>{busy.map(nameOf).join(', ')}</b> : <b>Handing off</b>}
                                {activeStage ? <> &middot; {activeStage.title}: {activeStage.job}</> : null}
                            </span>
                        </>
                    ) : finished ? (
                        <span className={`sw-status-text ${outcome === 'pass' ? 'is-pass' : outcome === 'fail' ? 'is-fail' : ''}`}>
                            {finished.stopped ? 'Stopped.' : finished.ok ? 'Mission complete: tested and reviewed.' : 'Finished with problems.'}
                        </span>
                    ) : (
                        <span className="sw-status-text sw-muted">
                            {crew.length} agents ready &middot; {model || source.hint} &middot; {source.label}
                        </span>
                    )}
                    {startedAt ? <span className="sw-clock">{clock(elapsed)}</span> : null}
                </div>
            </div>

            <nav className="sw-tabs" role="tablist" aria-label="Swarm sections">
                {(
                    [
                        ['mission', 'Mission', null],
                        ['crew', 'Crew', String(crew.length)],
                        ['model', 'Model', null],
                        ['live', 'Live', null],
                    ] as [Tab, string, string | null][]
                ).map(([id, label, count]) => (
                    <button
                        key={id}
                        type="button"
                        role="tab"
                        aria-selected={tab === id}
                        className={`sw-tab${tab === id ? ' is-on' : ''}`}
                        onClick={() => setTab(id)}
                    >
                        {label}
                        {count ? <span className="sw-tab-count">{count}</span> : null}
                        {id === 'live' && running ? <span className="sw-tab-live" aria-label="running" /> : null}
                    </button>
                ))}
            </nav>

            <div className="sw-body">
                {tab === 'mission' ? (
                    <MissionTab
                        goal={goal}
                        setGoal={setGoal}
                        crew={crew}
                        source={source}
                        model={model || source.hint}
                        status={status}
                        blocker={blocker}
                        onLaunch={launch}
                        go={setTab}
                    />
                ) : null}
                {tab === 'crew' ? <CrewTab crew={crew} setCrew={setCrew} locked={running} /> : null}
                {tab === 'model' ? (
                    <ModelTab
                        prefs={prefs}
                        setPrefs={setPrefs}
                        apiKey={apiKey}
                        setApiKey={setApiKey}
                        status={status}
                        locked={running}
                    />
                ) : null}
                {tab === 'live' ? <LiveTab state={state} nameOf={nameOf} elapsed={elapsed} /> : null}
            </div>

            <footer className="sw-foot">
                {running ? (
                    <button type="button" className="sw-stop" onClick={stop}>
                        <CircleStop size={15} /> Stop the swarm
                    </button>
                ) : state.phase === 'finished' ? (
                    <button type="button" className="sw-again" onClick={fresh}>
                        <Rocket size={15} /> New mission
                    </button>
                ) : (
                    <>
                        <button type="button" className="sw-launch" disabled={!!blocker} onClick={launch} title={blocker ?? 'Launch (Ctrl+Enter)'}>
                            <Rocket size={16} /> Launch {crew.length} agents
                        </button>
                        {blocker && goal.trim() ? <div className="sw-blocker">{blocker}</div> : null}
                    </>
                )}
            </footer>
        </aside>
    );
}

function MissionTab({
    goal,
    setGoal,
    crew,
    source,
    model,
    status,
    blocker,
    onLaunch,
    go,
}: {
    goal: string;
    setGoal: (g: string) => void;
    crew: CrewAgent[];
    source: SourceDef;
    model: string;
    status: SwarmStatus | null;
    blocker: string | null;
    onLaunch: () => void;
    go: (t: Tab) => void;
}) {
    const ready = sourceReady(source, status);
    return (
        <div className="sw-mission">
            <div className="sw-prompt">
                <Sparkles size={15} className="sw-prompt-icon" />
                <textarea
                    aria-label="Mission"
                    rows={3}
                    value={goal}
                    placeholder="What should the swarm build? Name the data if you know it."
                    onChange={e => setGoal(e.target.value)}
                    onKeyDown={e => {
                        if (e.key === 'Enter' && (e.metaKey || e.ctrlKey) && !blocker) onLaunch();
                    }}
                />
                <kbd className="sw-kbd">Ctrl &#9166;</kbd>
            </div>
            <div className="sw-suggest">
                {EXAMPLES.map(x => (
                    <button key={x} type="button" className="sw-chip" onClick={() => setGoal(x)}>
                        {x}
                    </button>
                ))}
            </div>
            <div className="sw-tiles">
                <button type="button" className="sw-tile" onClick={() => go('crew')}>
                    <span className="sw-tile-k">Crew</span>
                    <b>{crew.length} agents</b>
                    <span className="sw-dots">
                        {STAGES.map(st => (
                            <i
                                key={st.id}
                                title={`${st.title}: ${crew.filter(a => a.stage === st.id).length}`}
                                style={{ background: STAGE_COLOR[st.id], opacity: crew.some(a => a.stage === st.id) ? 1 : 0.25 }}
                            >
                                {crew.filter(a => a.stage === st.id).length || ''}
                            </i>
                        ))}
                    </span>
                </button>
                <button type="button" className="sw-tile" onClick={() => go('model')}>
                    <span className="sw-tile-k">Model</span>
                    <b className="sw-ellipsis">{model}</b>
                    <span className={`sw-tile-note ${ready.ok ? 'is-ok' : ''}`}>
                        {source.label} &middot; {ready.text}
                    </span>
                </button>
            </div>
        </div>
    );
}

/** Whether a source is ready to run, in a few words. */
function sourceReady(s: SourceDef, status: SwarmStatus | null): { ok: boolean; text: string } {
    const where = isWebBackend() ? 'on the server' : 'on this computer';
    if (s.id === 'claude-account') {
        const a = status?.anthropicAccount;
        if (!a) return { ok: false, text: 'Anthropic sign-in' };
        if (!a.installed) return { ok: false, text: `ant not installed ${where}` };
        return a.signedIn ? { ok: true, text: `signed in ${where}` } : { ok: false, text: 'run `ant auth login`' };
    }
    if (s.local) return { ok: true, text: 'local, no key' };
    if (s.keyEnv && status?.keys?.[s.keyEnv]) return { ok: true, text: `${s.keyEnv} found ${where}` };
    if (s.id === 'custom') return { ok: true, text: 'your endpoint' };
    return { ok: false, text: 'paste a key' };
}

function CrewTab({
    crew,
    setCrew,
    locked,
}: {
    crew: CrewAgent[];
    setCrew: (c: CrewAgent[]) => void;
    locked: boolean;
}) {
    const [open, setOpen] = useState<string | null>(null);
    const [adding, setAdding] = useState<StageId | null>(null);
    const [draft, setDraft] = useState({ name: '', brief: '' });
    const problem = crewProblem(crew);

    const add = (stage: StageId, name: string, brief: string) => {
        const id = newAgentId(crew, name);
        setCrew([...crew, { id, name: name.trim(), stage, brief: brief.trim() }]);
        setOpen(id);
    };
    const update = (id: string, patch: Partial<CrewAgent>) => setCrew(crew.map(a => (a.id === id ? { ...a, ...patch } : a)));
    const remove = (id: string) => setCrew(crew.filter(a => a.id !== id));

    return (
        <div className="sw-crew">
            <div className="sw-crew-head">
                <div>
                    <b>{crew.length} agents</b>
                    <span>As many as you like. Discover and Review work side by side, six at a time.</span>
                </div>
                <button type="button" className="sw-ghost" disabled={locked} onClick={() => setCrew(DEFAULT_CREW)} title="Back to one agent per stage">
                    <RotateCcw size={13} /> Reset
                </button>
            </div>
            <div className="sw-templates">
                {TEMPLATES.map(t => (
                    <button
                        key={t.name}
                        type="button"
                        className="sw-template"
                        disabled={locked}
                        style={{ ['--c' as string]: STAGE_COLOR[t.stage] }}
                        onClick={() => add(t.stage, t.name, t.brief)}
                        title={t.brief}
                    >
                        <Plus size={12} /> {t.name}
                    </button>
                ))}
            </div>
            {problem ? <div className="sw-warn">{problem}</div> : null}
            {STAGES.map(st => {
                const members = crew.filter(a => a.stage === st.id);
                const Icon = STAGE_ICON[st.id];
                return (
                    <section key={st.id} className="sw-stage" style={{ ['--c' as string]: STAGE_COLOR[st.id] }}>
                        <div className="sw-stage-head">
                            <span className="sw-stage-icon">
                                <Icon size={13} />
                            </span>
                            <b>{st.title}</b>
                            <span className="sw-stage-job">{st.job}</span>
                            {st.sideBySide ? <span className="sw-badge">side by side</span> : null}
                            <button
                                type="button"
                                className="sw-add"
                                disabled={locked}
                                onClick={() => {
                                    setAdding(adding === st.id ? null : st.id);
                                    setDraft({ name: '', brief: '' });
                                }}
                                aria-label={`Add an agent to ${st.title}`}
                            >
                                <Plus size={13} />
                            </button>
                        </div>
                        <div className="sw-members">
                            {members.length ? null : <span className="sw-none">No agent: this stage is skipped.</span>}
                            {members.map(a => (
                                <button
                                    key={a.id}
                                    type="button"
                                    className={`sw-member${open === a.id ? ' is-open' : ''}`}
                                    onClick={() => setOpen(open === a.id ? null : a.id)}
                                >
                                    <i />
                                    {a.name}
                                    {a.brief ? <em>&middot; own brief</em> : null}
                                </button>
                            ))}
                        </div>
                        {members
                            .filter(a => a.id === open)
                            .map(a => (
                                <div key={a.id} className="sw-editor">
                                    <input
                                        className="sw-input"
                                        value={a.name}
                                        disabled={locked || a.builtIn}
                                        onChange={e => update(a.id, { name: e.target.value })}
                                        aria-label="Agent name"
                                    />
                                    <textarea
                                        className="sw-input"
                                        rows={3}
                                        value={a.brief}
                                        disabled={locked}
                                        placeholder={`Its own brief, on top of the ${st.title} duty. Empty: just the duty.`}
                                        onChange={e => update(a.id, { brief: e.target.value })}
                                        aria-label="Agent brief"
                                    />
                                    <button type="button" className="sw-ghost sw-danger" disabled={locked} onClick={() => remove(a.id)}>
                                        <Trash2 size={13} /> Remove {a.name}
                                    </button>
                                </div>
                            ))}
                        {adding === st.id ? (
                            <div className="sw-editor">
                                <input
                                    className="sw-input"
                                    autoFocus
                                    value={draft.name}
                                    placeholder={`Name, e.g. ${st.id === 'review' ? 'Cost Auditor' : st.id === 'discover' ? 'Schema Scout' : 'Second ' + st.title + 'er'}`}
                                    onChange={e => setDraft({ ...draft, name: e.target.value })}
                                    aria-label="New agent name"
                                />
                                <textarea
                                    className="sw-input"
                                    rows={3}
                                    value={draft.brief}
                                    placeholder="What should it focus on?"
                                    onChange={e => setDraft({ ...draft, brief: e.target.value })}
                                    aria-label="New agent brief"
                                />
                                <div className="sw-row">
                                    <button
                                        type="button"
                                        className="sw-primary"
                                        disabled={!draft.name.trim()}
                                        onClick={() => {
                                            add(st.id, draft.name, draft.brief);
                                            setAdding(null);
                                        }}
                                    >
                                        <Plus size={13} /> Add to {st.title}
                                    </button>
                                    <button type="button" className="sw-ghost" onClick={() => setAdding(null)}>
                                        Cancel
                                    </button>
                                </div>
                            </div>
                        ) : null}
                    </section>
                );
            })}
        </div>
    );
}

function ModelTab({
    prefs,
    setPrefs,
    apiKey,
    setApiKey,
    status,
    locked,
}: {
    prefs: Prefs;
    setPrefs: (p: Prefs) => void;
    apiKey: string;
    setApiKey: (k: string) => void;
    status: SwarmStatus | null;
    locked: boolean;
}) {
    const source = SOURCES.find(s => s.id === prefs.source) ?? SOURCES[0];
    const model = prefs.models[prefs.source] ?? '';
    const takesKey = !!source.keyEnv || source.id === 'custom';
    const envHasKey = !!(source.keyEnv && status?.keys?.[source.keyEnv]);
    const where = isWebBackend() ? 'on the server' : 'on this computer';
    return (
        <div className="sw-model">
            <div className="sw-providers" role="radiogroup" aria-label="Where the model runs">
                {SOURCES.map(s => {
                    const ready = sourceReady(s, status);
                    const Icon = s.id === 'claude-account' ? UserRound : s.local ? Cpu : s.keyEnv ? KeyRound : Cloud;
                    return (
                        <button
                            key={s.id}
                            type="button"
                            role="radio"
                            aria-checked={prefs.source === s.id}
                            disabled={locked}
                            className={`sw-provider${prefs.source === s.id ? ' is-on' : ''}`}
                            onClick={() => setPrefs({ ...prefs, source: s.id })}
                        >
                            <Icon size={14} />
                            <span>
                                <b>{s.label}</b>
                                <small>{s.sub}</small>
                                <small className={ready.ok ? 'is-ok' : ''}>{ready.text}</small>
                            </span>
                        </button>
                    );
                })}
            </div>
            <label className="sw-label" htmlFor="sw-model-id">
                Model
            </label>
            <input
                id="sw-model-id"
                className="sw-input"
                list={source.models.length ? `sw-models-${source.id}` : undefined}
                value={model}
                disabled={locked}
                placeholder={source.hint}
                onChange={e => setPrefs({ ...prefs, models: { ...prefs.models, [prefs.source]: e.target.value } })}
            />
            {source.models.length ? (
                <datalist id={`sw-models-${source.id}`}>
                    {source.models.map(m => (
                        <option key={m} value={m} />
                    ))}
                </datalist>
            ) : null}
            {source.id === 'custom' ? (
                <>
                    <label className="sw-label" htmlFor="sw-base">
                        Base URL
                    </label>
                    <input
                        id="sw-base"
                        className="sw-input"
                        value={prefs.baseUrl}
                        disabled={locked}
                        placeholder="https://api.example.com/v1"
                        onChange={e => setPrefs({ ...prefs, baseUrl: e.target.value })}
                    />
                </>
            ) : null}
            {takesKey ? (
                <>
                    <label className="sw-label" htmlFor="sw-key">
                        API key
                    </label>
                    <input
                        id="sw-key"
                        className="sw-input"
                        type="password"
                        autoComplete="off"
                        value={apiKey}
                        disabled={locked}
                        placeholder={envHasKey ? `${source.keyEnv} is set ${where}; paste one to use another` : 'For this run only, never saved'}
                        onChange={e => setApiKey(e.target.value)}
                    />
                </>
            ) : null}
            <p className="sw-fine">
                {source.id === 'claude-account'
                    ? "Anthropic's own sign-in (ant auth login): no key to paste, and Duckle never sees a credential."
                    : source.local
                      ? 'Runs on this machine: nothing leaves it. The model needs tool calling.'
                      : 'A pasted key lasts one run and is never saved. A key from the environment only goes to its own provider.'}
            </p>
        </div>
    );
}

function LiveTab({ state, nameOf, elapsed }: { state: SwarmState; nameOf: (id: string) => string; elapsed: number }) {
    const feed = useRef<HTMLDivElement | null>(null);
    useEffect(() => {
        const el = feed.current;
        if (el) el.scrollTop = el.scrollHeight;
    }, [state.stints]);
    const crewOf = (id: string) => state.crew.find(a => a.id === id);
    const done = state.finished;

    if (state.phase === 'idle') {
        return (
            <div className="sw-empty">
                <Rocket size={22} />
                <b>Nothing running yet</b>
                <span>Launch a mission and watch the crew work here.</span>
            </div>
        );
    }
    return (
        <div className="sw-live">
            <div className="sw-steps">
                {STAGES.map(st => {
                    const Icon = STAGE_ICON[st.id];
                    const used = state.crew.some(a => a.stage === st.id);
                    return (
                        <div
                            key={st.id}
                            className={`sw-step sw-${state.stages[st.id]}${used ? '' : ' is-unused'}`}
                            style={{ ['--c' as string]: STAGE_COLOR[st.id] }}
                            title={`${st.title}: ${st.job}`}
                        >
                            <Icon size={13} />
                            <span>{st.title}</span>
                        </div>
                    );
                })}
            </div>
            <div className="sw-stats">
                <Stat label="tool calls" value={String(state.toolCalls)} />
                <Stat label="steps" value={String(state.nodeCount)} />
                <Stat label="gates" value={String(state.gateCount)} />
                <Stat label="rows" value={state.rows === null ? '-' : compact(state.rows)} />
                <Stat label="tokens" value={compact(state.inputTokens + state.outputTokens)} />
            </div>
            {state.model ? <div className="sw-model-line">{state.model}</div> : null}
            <div className="sw-feed" ref={feed}>
                <div className="sw-goal">
                    <Sparkles size={13} /> {state.goal}
                </div>
                {state.stints.map((st, i) => {
                    const member = crewOf(st.agent);
                    const stage = member?.stage ?? 'build';
                    return (
                        <section key={i} className={`sw-stint sw-stint-${st.state}`} style={{ ['--c' as string]: STAGE_COLOR[stage] }}>
                            <div className="sw-stint-head">
                                <span className="sw-who">{nameOf(st.agent)}</span>
                                <span className="sw-where">{STAGES.find(s => s.id === stage)?.title}</span>
                                {st.verdict ? <span className={`sw-verdict is-${st.verdict}`}>{st.verdict === 'pass' ? 'PASS' : 'FAIL'}</span> : null}
                                {st.state === 'working' ? <span className="sw-working">working</span> : null}
                            </div>
                            {st.tools.length ? (
                                <div className="sw-calls">
                                    {st.tools.map(t => (
                                        <span key={t.id} className={`sw-call is-${t.state}`} title={t.preview ?? ''}>
                                            <span className="sw-call-name">{t.name}</span>
                                            {t.about ? <span className="sw-call-about">{t.about}</span> : null}
                                            {t.ms !== undefined ? <span className="sw-call-ms">{t.ms < 1000 ? `${t.ms}ms` : `${(t.ms / 1000).toFixed(1)}s`}</span> : null}
                                        </span>
                                    ))}
                                </div>
                            ) : null}
                            {st.state === 'working' && (st.thinking || st.text) ? (
                                <div className="sw-stream">
                                    {st.thinking ? <div className="sw-thinking">{st.thinking}</div> : null}
                                    {st.text ? <div className="sw-said">{st.text}</div> : null}
                                    <span className="sw-caret" />
                                </div>
                            ) : null}
                            {st.summary && st.state !== 'working' ? <div className="sw-summary">{st.summary}</div> : null}
                        </section>
                    );
                })}
                {state.error ? <div className="sw-error">{state.error}</div> : null}
                {done && !done.stopped ? (
                    <div className={`sw-report ${done.ok ? 'is-pass' : 'is-fail'}`}>
                        <div className="sw-report-badge">{done.ok ? 'TESTED & REVIEWED' : 'NEEDS ANOTHER LOOK'}</div>
                        <div className="sw-report-meta">
                            {clock(done.durationMs ?? elapsed)} &middot; {done.agents ?? state.crew.length} agents &middot;{' '}
                            {done.toolCalls ?? state.toolCalls} tool calls &middot; {state.runs} run{state.runs === 1 ? '' : 's'} &middot;{' '}
                            {compact((done.inputTokens ?? 0) + (done.outputTokens ?? 0))} tokens
                        </div>
                    </div>
                ) : null}
            </div>
        </div>
    );
}

function Stat({ label, value }: { label: string; value: string }) {
    return (
        <div className="sw-stat">
            <b>{value}</b>
            <span>{label}</span>
        </div>
    );
}
