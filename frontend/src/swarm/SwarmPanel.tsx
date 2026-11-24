// Swarm mode: describe a goal, and six agents build, run and test the pipeline
// in front of you. The pipeline appears on the canvas as the Builder writes it;
// the Tester's runs light up its steps with row counts.

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { CircleStop, KeyRound, Rocket, Sparkles, UserRound, X } from 'lucide-react';
import SwarmConstellation, { ROLE_COLOR, type Flight } from './SwarmConstellation';
import {
    ROLES,
    applySwarmEvent,
    compact,
    initialSwarmState,
    type RoleId,
    type SwarmEvent,
    type SwarmState,
} from './swarm-state';
import { swarmRun, swarmStatus, swarmStop, type ProviderKind, type SwarmProvider, type SwarmStatus } from './swarm-api';
import { isWebBackend } from '../web-fs';
import './swarm.css';

/** Where a model is reached. Claude has two ways in: your account or a key. */
type Source = 'claude' | 'openai' | 'huggingface' | 'ollama' | 'lmstudio' | 'openrouter' | 'custom';

const SOURCES: { id: Source; label: string; base?: string; keyEnv?: string; model: string }[] = [
    { id: 'claude', label: 'Claude', model: 'claude-opus-5-5' },
    { id: 'openai', label: 'OpenAI', base: 'https://api.openai.com/v1', keyEnv: 'OPENAI_API_KEY', model: '' },
    { id: 'huggingface', label: 'Hugging Face', base: 'https://router.huggingface.co/v1', keyEnv: 'HF_TOKEN', model: '' },
    { id: 'ollama', label: 'Ollama', base: 'http://localhost:11434/v1', model: '' },
    { id: 'lmstudio', label: 'LM Studio', base: 'http://localhost:1234/v1', model: '' },
    { id: 'openrouter', label: 'OpenRouter', base: 'https://openrouter.ai/api/v1', keyEnv: 'OPENROUTER_API_KEY', model: '' },
    { id: 'custom', label: 'Custom', model: '' },
];

const MODEL_HINT: Record<Source, string> = {
    claude: 'claude-opus-5-5',
    openai: 'a model id from your OpenAI account',
    huggingface: 'e.g. Qwen/Qwen3-Coder-480B-A35B-Instruct',
    ollama: 'a pulled model with tool calling',
    lmstudio: 'the loaded model',
    openrouter: 'a model id from openrouter.ai/models',
    custom: 'model id',
};

const CLAUDE_MODELS = ['claude-opus-5-5', 'claude-sonnet-5-5', 'claude-haiku-5-5', 'claude-fable-5-2'];

const EXAMPLES = [
    'Total revenue per customer from the orders file, newest year only, written to Parquet.',
    'Clean the customers file: one row per email, trimmed names, country codes in upper case.',
    'Join orders to products and report units and revenue per category per month.',
];

type Prefs = { source: Source; claudeVia: 'account' | 'key'; model: string; baseUrl: string };

const PREFS_KEY = 'duckle.swarm.provider';

function loadPrefs(): Prefs {
    const fallback: Prefs = { source: 'claude', claudeVia: 'account', model: '', baseUrl: '' };
    try {
        const raw = localStorage.getItem(PREFS_KEY);
        return raw ? { ...fallback, ...(JSON.parse(raw) as Partial<Prefs>) } : fallback;
    } catch {
        return fallback;
    }
}

function savePrefs(p: Prefs): void {
    try {
        localStorage.setItem(PREFS_KEY, JSON.stringify(p));
    } catch {
        // A private window: the choice lasts this session.
    }
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
    /** The Builder or Fixer wrote the pipeline: show it on the canvas. */
    onPipeline: (id: string, name: string, doc: Record<string, unknown>) => void;
    /** The Tester ran it: show each step's result on the canvas. */
    onRun: (id: string, status: string, durationMs: number, nodes: SwarmRunNodes) => void;
    /** Whether a swarm is running, for the canvas's live styling. */
    onLive: (live: boolean) => void;
}) {
    const [state, setState] = useState<SwarmState>(initialSwarmState);
    const [goal, setGoal] = useState('');
    const [prefs, setPrefs] = useState<Prefs>(loadPrefs);
    const [apiKey, setApiKey] = useState('');
    const [status, setStatus] = useState<SwarmStatus | null>(null);
    const [flights, setFlights] = useState<Flight[]>([]);
    const [busyTool, setBusyTool] = useState<string | null>(null);
    const [startedAt, setStartedAt] = useState<number | null>(null);
    const [tick, setTick] = useState(0);
    const abort = useRef<AbortController | null>(null);
    const pipelineId = useRef<string | null>(null);
    const feed = useRef<HTMLDivElement | null>(null);

    const running = state.phase === 'running';
    const source = SOURCES.find(s => s.id === prefs.source) ?? SOURCES[0];

    useEffect(() => {
        void swarmStatus().then(setStatus);
    }, []);

    useEffect(() => savePrefs(prefs), [prefs]);

    useEffect(() => {
        if (!running) return;
        const t = window.setInterval(() => setTick(n => n + 1), 1000);
        return () => window.clearInterval(t);
    }, [running]);

    useEffect(() => {
        onLive(running);
        return () => onLive(false);
    }, [running, onLive]);

    useEffect(() => {
        const el = feed.current;
        if (el) el.scrollTop = el.scrollHeight;
    }, [state.stints, state.stream]);

    const provider = useMemo((): SwarmProvider => {
        const model = prefs.model.trim();
        if (prefs.source === 'claude') {
            const kind: ProviderKind = prefs.claudeVia === 'account' ? 'anthropic-account' : 'anthropic-key';
            return { kind, model: model || undefined, apiKey: prefs.claudeVia === 'key' ? apiKey || undefined : undefined };
        }
        const baseUrl = prefs.source === 'custom' ? prefs.baseUrl.trim() : source.base;
        return { kind: 'openai-compatible', model, baseUrl, apiKey: apiKey || undefined };
    }, [prefs, apiKey, source]);

    /** Why Launch cannot go yet, if it cannot. */
    const blocker = useMemo((): string | null => {
        if (!goal.trim()) return 'Describe what the pipeline should do.';
        if (!workspacePath && !isWebBackend()) return 'Open a workspace first.';
        if (prefs.source === 'claude' && prefs.claudeVia === 'account' && status && !status.anthropicAccount.installed)
            return "Install Anthropic's CLI (ant) to use your account, or switch to an API key.";
        if (prefs.source !== 'claude' && !prefs.model.trim()) return 'Name the model to use.';
        if (prefs.source === 'custom' && !prefs.baseUrl.trim()) return "Give the endpoint's base URL.";
        return null;
    }, [goal, workspacePath, prefs, status]);

    const handle = useCallback(
        (e: SwarmEvent) => {
            setState(s => applySwarmEvent(s, e));
            switch (e.type) {
                case 'started':
                    pipelineId.current = e.pipelineId;
                    break;
                case 'tool':
                    setBusyTool(e.name);
                    setFlights(f => [...f, { kind: 'call', role: e.role }]);
                    break;
                case 'toolResult':
                    setBusyTool(null);
                    setFlights(f => [...f, { kind: 'answer', role: e.role, ok: e.ok }]);
                    break;
                case 'handoff':
                    setFlights(f => [...f, { kind: 'handoff', from: e.from, to: e.to }]);
                    break;
                case 'pipeline':
                    onPipeline(e.id, e.name, e.doc as Record<string, unknown>);
                    break;
                case 'run':
                    if (pipelineId.current && e.nodes) {
                        onRun(pipelineId.current, e.status, e.durationMs ?? 0, e.nodes);
                    }
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
        const ctl = new AbortController();
        abort.current = ctl;
        await swarmRun({ goal: goal.trim(), workspace: workspacePath ?? '', provider }, handle, ctl.signal);
        abort.current = null;
        // A run that ended without its finished event (a transport failure)
        // still has to leave the running state.
        setState(s => (s.phase === 'running' ? { ...s, phase: 'finished' } : s));
    }, [blocker, running, goal, workspacePath, provider, handle]);

    const stop = useCallback(() => {
        abort.current?.abort();
        void swarmStop();
    }, []);

    const elapsed = startedAt ? (state.finished?.durationMs ?? Date.now() - startedAt) : 0;
    void tick;

    const activeRole = ROLES.find(r => r.id === state.active);
    const stream = state.stream;
    const verdict = state.finished;

    return (
        <aside className={`swarm-panel${running ? ' is-running' : ''}`} aria-label="Agent swarm">
            <header className="swarm-head">
                <div className="swarm-title">
                    <span className="swarm-hex" aria-hidden="true" />
                    <span className="swarm-word">SWARM</span>
                    <span className="swarm-sub">six agents, one goal, a tested pipeline</span>
                </div>
                <button type="button" className="swarm-x" onClick={onClose} aria-label="Close the swarm" title="Close">
                    <X size={15} />
                </button>
            </header>

            <div className="swarm-stage">
                <SwarmConstellation agents={state.agents} active={state.active} busyTool={busyTool} flights={flights} />
                <div className="swarm-status-line">
                    {running && activeRole ? (
                        <>
                            <span className="swarm-dot" style={{ background: ROLE_COLOR[activeRole.id][0] }} />
                            <b>{activeRole.title}</b>&nbsp;{activeRole.job}
                        </>
                    ) : verdict ? (
                        <span>{verdict.stopped ? 'Stopped.' : verdict.ok ? 'Done: tested and reviewed.' : 'Finished with problems.'}</span>
                    ) : (
                        <span className="swarm-muted">Idle. Give the swarm a goal.</span>
                    )}
                    {startedAt ? <span className="swarm-clock">{clock(elapsed)}</span> : null}
                </div>
                <div className="swarm-stats">
                    <Stat label="tool calls" value={String(state.toolCalls)} />
                    <Stat label="steps" value={String(state.nodeCount)} />
                    <Stat label="quality gates" value={String(state.gateCount)} />
                    <Stat label="rows" value={state.rows === null ? '-' : compact(state.rows)} />
                    <Stat label="tokens" value={compact(state.inputTokens + state.outputTokens)} />
                </div>
            </div>

            {state.phase === 'idle' ? (
                <Compose
                    goal={goal}
                    setGoal={setGoal}
                    prefs={prefs}
                    setPrefs={setPrefs}
                    apiKey={apiKey}
                    setApiKey={setApiKey}
                    status={status}
                    blocker={blocker}
                    onLaunch={launch}
                />
            ) : (
                <>
                    {state.model ? <div className="swarm-model">{state.model}</div> : null}
                    <div className="swarm-feed" ref={feed}>
                        <div className="swarm-goal-echo">
                            <Sparkles size={13} /> {state.goal}
                        </div>
                        {state.stints.map((st, i) => {
                            const role = ROLES.find(r => r.id === st.role)!;
                            const live = st.state === 'working' && stream?.role === st.role;
                            return (
                                <section key={i} className={`swarm-stint swarm-stint-${st.state}`} style={{ ['--role' as string]: ROLE_COLOR[st.role][0] }}>
                                    <div className="swarm-stint-head">
                                        <span className="swarm-role-pill">{role.title}</span>
                                        {st.verdict ? (
                                            <span className={`swarm-verdict swarm-verdict-${st.verdict}`}>{st.verdict === 'pass' ? 'PASS' : 'FAIL'}</span>
                                        ) : null}
                                        {st.state === 'working' ? <span className="swarm-working">working</span> : null}
                                    </div>
                                    {st.tools.length ? (
                                        <div className="swarm-chips">
                                            {st.tools.map(t => (
                                                <span key={t.id} className={`swarm-chip swarm-chip-${t.state}`} title={t.preview ?? ''}>
                                                    <span className="swarm-chip-name">{t.name}</span>
                                                    {t.about ? <span className="swarm-chip-about">{t.about}</span> : null}
                                                    {t.ms !== undefined ? <span className="swarm-chip-ms">{t.ms < 1000 ? `${t.ms}ms` : `${(t.ms / 1000).toFixed(1)}s`}</span> : null}
                                                </span>
                                            ))}
                                        </div>
                                    ) : null}
                                    {live && (stream.thinking || stream.text) ? (
                                        <div className="swarm-stream">
                                            {stream.thinking ? <div className="swarm-thinking">{stream.thinking}</div> : null}
                                            {stream.text ? <div className="swarm-said">{stream.text}</div> : null}
                                            <span className="swarm-caret" />
                                        </div>
                                    ) : null}
                                    {st.summary && st.state !== 'working' ? <div className="swarm-summary">{st.summary}</div> : null}
                                </section>
                            );
                        })}
                        {state.error ? <div className="swarm-error">{state.error}</div> : null}
                        {verdict && !verdict.stopped ? (
                            <div className={`swarm-report ${verdict.ok ? 'is-pass' : 'is-fail'}`}>
                                <div className="swarm-report-badge">{verdict.ok ? 'TESTED & REVIEWED' : 'NEEDS ANOTHER LOOK'}</div>
                                <div className="swarm-report-meta">
                                    {clock(verdict.durationMs ?? elapsed)} &middot; {verdict.toolCalls ?? state.toolCalls} tool calls &middot;{' '}
                                    {state.runs} run{state.runs === 1 ? '' : 's'} &middot; {compact((verdict.inputTokens ?? 0) + (verdict.outputTokens ?? 0))} tokens
                                </div>
                            </div>
                        ) : null}
                    </div>
                    <footer className="swarm-foot">
                        {running ? (
                            <button type="button" className="swarm-stop" onClick={stop}>
                                <CircleStop size={14} /> Stop
                            </button>
                        ) : (
                            <button
                                type="button"
                                className="swarm-again"
                                onClick={() => {
                                    setState(initialSwarmState());
                                    setStartedAt(null);
                                    setFlights([]);
                                }}
                            >
                                <Rocket size={14} /> New goal
                            </button>
                        )}
                    </footer>
                </>
            )}
        </aside>
    );
}

function Stat({ label, value }: { label: string; value: string }) {
    return (
        <div className="swarm-stat">
            <b>{value}</b>
            <span>{label}</span>
        </div>
    );
}

function Compose({
    goal,
    setGoal,
    prefs,
    setPrefs,
    apiKey,
    setApiKey,
    status,
    blocker,
    onLaunch,
}: {
    goal: string;
    setGoal: (g: string) => void;
    prefs: Prefs;
    setPrefs: (p: Prefs) => void;
    apiKey: string;
    setApiKey: (k: string) => void;
    status: SwarmStatus | null;
    blocker: string | null;
    onLaunch: () => void;
}) {
    const source = SOURCES.find(s => s.id === prefs.source) ?? SOURCES[0];
    const claude = prefs.source === 'claude';
    const envKey = claude ? 'ANTHROPIC_API_KEY' : source.keyEnv;
    const envHasKey = !!(envKey && status?.keys?.[envKey]);
    const account = status?.anthropicAccount;
    const where = isWebBackend() ? 'on the server' : 'on this computer';

    return (
        <div className="swarm-compose">
            <label className="swarm-field-label" htmlFor="swarm-goal">
                Goal
            </label>
            <textarea
                id="swarm-goal"
                className="swarm-goal"
                rows={3}
                value={goal}
                placeholder="What should the pipeline do? Name the data if you know it."
                onChange={e => setGoal(e.target.value)}
                onKeyDown={e => {
                    if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) onLaunch();
                }}
            />
            <div className="swarm-examples">
                {EXAMPLES.map(x => (
                    <button key={x} type="button" className="swarm-example" onClick={() => setGoal(x)}>
                        {x}
                    </button>
                ))}
            </div>

            <div className="swarm-field-label">Model</div>
            <div className="swarm-sources" role="radiogroup" aria-label="Where the model runs">
                {SOURCES.map(s => (
                    <button
                        key={s.id}
                        type="button"
                        role="radio"
                        aria-checked={prefs.source === s.id}
                        className={`swarm-source${prefs.source === s.id ? ' is-on' : ''}`}
                        onClick={() => setPrefs({ ...prefs, source: s.id, model: s.id === prefs.source ? prefs.model : '' })}
                    >
                        {s.label}
                    </button>
                ))}
            </div>

            {claude ? (
                <div className="swarm-via" role="radiogroup" aria-label="How to reach Claude">
                    <button
                        type="button"
                        role="radio"
                        aria-checked={prefs.claudeVia === 'account'}
                        className={`swarm-via-opt${prefs.claudeVia === 'account' ? ' is-on' : ''}`}
                        onClick={() => setPrefs({ ...prefs, claudeVia: 'account' })}
                    >
                        <UserRound size={14} />
                        <span>
                            <b>Your Anthropic account</b>
                            <small>
                                {!account
                                    ? 'Signs in through Anthropic. No key to paste.'
                                    : !account.installed
                                      ? `Anthropic's CLI (ant) is not installed ${where}.`
                                      : account.signedIn
                                        ? `Signed in ${where}.`
                                        : `Not signed in yet: run \`ant auth login\` ${where}.`}
                            </small>
                        </span>
                    </button>
                    <button
                        type="button"
                        role="radio"
                        aria-checked={prefs.claudeVia === 'key'}
                        className={`swarm-via-opt${prefs.claudeVia === 'key' ? ' is-on' : ''}`}
                        onClick={() => setPrefs({ ...prefs, claudeVia: 'key' })}
                    >
                        <KeyRound size={14} />
                        <span>
                            <b>API key</b>
                            <small>{envHasKey ? `ANTHROPIC_API_KEY is set ${where}.` : 'Paste a key from the Claude Console.'}</small>
                        </span>
                    </button>
                </div>
            ) : null}

            <div className="swarm-row">
                <input
                    className="swarm-input"
                    list={claude ? 'swarm-claude-models' : undefined}
                    value={prefs.model}
                    placeholder={MODEL_HINT[prefs.source]}
                    onChange={e => setPrefs({ ...prefs, model: e.target.value })}
                    aria-label="Model id"
                />
                {claude ? (
                    <datalist id="swarm-claude-models">
                        {CLAUDE_MODELS.map(m => (
                            <option key={m} value={m} />
                        ))}
                    </datalist>
                ) : null}
            </div>
            {prefs.source === 'custom' ? (
                <div className="swarm-row">
                    <input
                        className="swarm-input"
                        value={prefs.baseUrl}
                        placeholder="Base URL, e.g. https://api.example.com/v1"
                        onChange={e => setPrefs({ ...prefs, baseUrl: e.target.value })}
                        aria-label="Base URL"
                    />
                </div>
            ) : null}
            {(!claude || prefs.claudeVia === 'key') ? (
                <div className="swarm-row">
                    <input
                        className="swarm-input"
                        type="password"
                        autoComplete="off"
                        value={apiKey}
                        placeholder={
                            envHasKey
                                ? `Using ${envKey} ${where}, or paste another`
                                : source.base?.startsWith('http://localhost')
                                  ? 'No key needed for a local server'
                                  : 'API key, for this run only'
                        }
                        onChange={e => setApiKey(e.target.value)}
                        aria-label="API key"
                    />
                </div>
            ) : null}
            <p className="swarm-fine">
                A key you paste is used for this run and never saved. The model needs tool calling.
            </p>

            <button type="button" className="swarm-launch" disabled={!!blocker} onClick={onLaunch} title={blocker ?? 'Launch (Ctrl+Enter)'}>
                <Rocket size={16} /> Launch the swarm
            </button>
            {blocker && goal.trim() ? <div className="swarm-blocker">{blocker}</div> : null}
            <ol className="swarm-legend">
                {ROLES.map(r => (
                    <li key={r.id} style={{ ['--role' as string]: ROLE_COLOR[r.id as RoleId][0] }}>
                        <b>{r.title}</b> {r.job}
                    </li>
                ))}
            </ol>
        </div>
    );
}
