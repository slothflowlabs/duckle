// The swarm at a glance: every agent in orbit around the Duckle engine.
//
// The orbit is cut into the six stages, and each agent sits in its stage's
// sector, so a crew of any size reads at once: a crowded sector shrinks its
// agents and spills onto an inner ring. The working agents glow; each tool
// call flies to the engine as a pixel and its answer flies back (blue when it
// worked, red when it did not); a hand-off sends a comet along the orbit, one
// to each agent of a stage that fans out. Every colour is a theme token, so
// the light theme gets the same picture in its own palette.
//
// Particles are placed from requestAnimationFrame, so they start the moment
// their event arrives rather than on SVG's own document clock.

import { useEffect, useMemo, useRef, useState } from 'react';
import { DraftingCompass, FlaskConical, Hammer, ScanSearch, ShieldCheck, Wrench } from 'lucide-react';
import { DuckleLogo } from '../workflow-ui/DuckleLogo';
import type { AgentState, StageId, StageState } from './swarm-state';

const W = 460;
const H = 252;
const CX = 230;
const CY = 128;
const RX = 158;
const RY = 84;
/** Where an overflowing sector's second ring sits, as a share of the orbit. */
const INNER = 0.74;

/** Each stage's sector of the orbit, in degrees (0 = right, clockwise). */
const SECTOR: Record<StageId, [number, number]> = {
    discover: [150, 210],
    design: [210, 270],
    build: [270, 330],
    test: [330, 390],
    repair: [30, 90],
    review: [90, 150],
};

/** A stage's colour: theme tokens only, so both themes stay on brand. */
export const STAGE_COLOR: Record<StageId, string> = {
    discover: 'var(--kind-source)',
    design: 'var(--kind-control)',
    build: 'var(--accent)',
    test: 'var(--success)',
    repair: 'var(--kind-sink)',
    review: 'var(--kind-control)',
};

export const STAGE_ICON = {
    discover: ScanSearch,
    design: DraftingCompass,
    build: Hammer,
    test: FlaskConical,
    repair: Wrench,
    review: ShieldCheck,
} as const;

const STAGE_TITLE: Record<StageId, string> = {
    discover: 'DISCOVER',
    design: 'DESIGN',
    build: 'BUILD',
    test: 'TEST',
    repair: 'REPAIR',
    review: 'REVIEW',
};

function at(deg: number, scale = 1): [number, number] {
    const r = (deg * Math.PI) / 180;
    return [CX + RX * scale * Math.cos(r), CY + RY * scale * Math.sin(r)];
}

type Seat = { id: string; name: string; stage: StageId; deg: number; ring: number; r: number; label: boolean };

/** Where each agent sits: spread across its stage's sector, two rings when crowded. */
export function seats(crew: { id: string; name: string; stage: StageId }[]): Seat[] {
    const out: Seat[] = [];
    for (const stage of Object.keys(SECTOR) as StageId[]) {
        const members = crew.filter(a => a.stage === stage);
        const k = members.length;
        if (!k) continue;
        const [a0, a1] = SECTOR[stage];
        const rings = k > 6 ? 2 : 1;
        const perRing = Math.ceil(k / rings);
        // An orb shrinks to keep its neighbours apart: about 134 px of arc per sector.
        const r = Math.max(4, Math.min(16, (134 / perRing - 6) / 2));
        members.forEach((m, i) => {
            const ring = rings === 2 ? i % 2 : 0;
            const slot = rings === 2 ? Math.floor(i / 2) : i;
            const count = rings === 2 ? (ring === 0 ? Math.ceil(k / 2) : Math.floor(k / 2)) : k;
            const deg = a0 + ((slot + 0.5) * (a1 - a0)) / count;
            out.push({ id: m.id, name: m.name, stage, deg, ring, r, label: k <= 2 });
        });
    }
    return out;
}

export type Flight =
    | { kind: 'call' | 'answer'; agent: string; ok?: boolean }
    | { kind: 'handoff'; from: string; to: string };

type Particle = Flight & { id: number; born: number; dur: number };

function seatPoint(s: Seat): [number, number] {
    return at(s.deg, s.ring ? INNER : 1);
}

/** Where an agent's name goes: on the engine's side of its orb, so the stage
 *  names outside the orbit never meet it. */
function namePoint(s: Seat): { x: number; y: number; anchor: 'start' | 'middle' | 'end' } {
    const [x, y] = seatPoint(s);
    const dx = CX - x;
    const dy = CY - y;
    const len = Math.hypot(dx, dy) || 1;
    const gap = s.r + 9;
    const horizontal = Math.abs(dx) / RX > Math.abs(dy) / RY;
    return {
        x: x + (dx / len) * gap,
        y: y + (dy / len) * gap + 3,
        anchor: horizontal ? (dx > 0 ? 'start' : 'end') : 'middle',
    };
}

/** A point along a flight, t from 0 to 1. */
function pointOf(p: Particle, t: number, byId: Map<string, Seat>): [number, number] | null {
    if (p.kind === 'handoff') {
        const a = byId.get(p.from);
        const b = byId.get(p.to);
        if (!a || !b) return null;
        // Along the orbit, the short way round.
        let d = b.deg - a.deg;
        if (d > 180) d -= 360;
        if (d < -180) d += 360;
        const scale = (a.ring ? INNER : 1) + ((b.ring ? INNER : 1) - (a.ring ? INNER : 1)) * t;
        return at(a.deg + d * t, scale);
    }
    const s = byId.get(p.agent);
    if (!s) return null;
    const [ax, ay] = seatPoint(s);
    const [x0, y0, x1, y1] = p.kind === 'call' ? [ax, ay, CX, CY] : [CX, CY, ax, ay];
    // A gentle bow, outbound and return on opposite sides.
    const mx = (x0 + x1) / 2 + (y1 - y0) * 0.18;
    const my = (y0 + y1) / 2 - (x1 - x0) * 0.18;
    const u = 1 - t;
    return [u * u * x0 + 2 * u * t * mx + t * t * x1, u * u * y0 + 2 * u * t * my + t * t * y1];
}

function colorOf(p: Particle, byId: Map<string, Seat>): string {
    if (p.kind === 'answer') return p.ok === false ? 'var(--danger)' : 'var(--success)';
    const s = byId.get(p.kind === 'handoff' ? p.from : p.agent);
    return s ? STAGE_COLOR[s.stage] : 'var(--accent)';
}

/** Background stars, fixed so they do not jump on re-render. */
const STARS: [number, number, number][] = Array.from({ length: 52 }, (_, i) => {
    const r = (n: number) => (((Math.sin(i * 12.9898 + n * 78.233) * 43758.5453) % 1) + 1) % 1;
    return [r(1) * W, r(2) * H, 0.5 + r(3) * 1.1];
});

export default function SwarmConstellation({
    crew,
    agents,
    stages,
    running,
    busyTool,
    flights,
    outcome,
}: {
    crew: { id: string; name: string; stage: StageId }[];
    agents: Record<string, AgentState>;
    stages: Record<StageId, StageState>;
    running: boolean;
    /** The tool being called right now, shown under the engine. */
    busyTool: string | null;
    /** Flights to launch: each new entry (by index) starts once. */
    flights: Flight[];
    /** How the last run ended, for the finale. */
    outcome: 'pass' | 'fail' | null;
}) {
    const placed = useMemo(() => seats(crew), [crew]);
    const byId = useMemo(() => new Map(placed.map(s => [s.id, s])), [placed]);
    const [particles, setParticles] = useState<Particle[]>([]);
    const [now, setNow] = useState(() => performance.now());
    const launched = useRef(0);
    const seq = useRef(0);

    useEffect(() => {
        if (flights.length < launched.current) launched.current = 0;
        if (flights.length === launched.current) return;
        const fresh = flights.slice(launched.current).map(f => ({
            ...f,
            id: ++seq.current,
            born: performance.now(),
            dur: f.kind === 'handoff' ? 1150 : 720,
        }));
        launched.current = flights.length;
        setParticles(ps => [...ps, ...fresh].slice(-60));
    }, [flights]);

    const anyParticles = particles.length > 0;
    useEffect(() => {
        if (!anyParticles) return;
        let raf = 0;
        const tick = () => {
            const t = performance.now();
            setNow(t);
            setParticles(ps => {
                const alive = ps.filter(p => t - p.born < p.dur);
                return alive.length === ps.length ? ps : alive;
            });
            raf = requestAnimationFrame(tick);
        };
        raf = requestAnimationFrame(tick);
        return () => cancelAnimationFrame(raf);
    }, [anyParticles]);

    return (
        <svg
            className={`sw-sky${running ? ' is-running' : ''}${outcome ? ` is-${outcome}` : ''}`}
            viewBox={`0 0 ${W} ${H}`}
            role="img"
            aria-label={`The agent swarm: ${crew.length} agents`}
        >
            <defs>
                <radialGradient id="sw-glow-core" cx="50%" cy="52%" r="62%">
                    <stop offset="0%" style={{ stopColor: 'var(--accent)', stopOpacity: 0.2 }} />
                    <stop offset="55%" style={{ stopColor: 'var(--accent)', stopOpacity: 0.05 }} />
                    <stop offset="100%" style={{ stopColor: 'var(--accent)', stopOpacity: 0 }} />
                </radialGradient>
                <linearGradient id="sw-sweep" x1="0" y1="0" x2="1" y2="0">
                    <stop offset="0%" style={{ stopColor: 'var(--accent)', stopOpacity: 0 }} />
                    <stop offset="100%" style={{ stopColor: 'var(--accent)', stopOpacity: 0.28 }} />
                </linearGradient>
                <filter id="sw-blur" x="-60%" y="-60%" width="220%" height="220%">
                    <feGaussianBlur stdDeviation="2.2" result="b" />
                    <feMerge>
                        <feMergeNode in="b" />
                        <feMergeNode in="SourceGraphic" />
                    </feMerge>
                </filter>
            </defs>

            <rect x="0" y="0" width={W} height={H} rx="14" className="sw-sky-bg" />
            <rect x="0" y="0" width={W} height={H} rx="14" fill="url(#sw-glow-core)" />
            {STARS.map(([x, y, r], i) => (
                <rect
                    key={i}
                    x={x}
                    y={y}
                    width={r}
                    height={r}
                    className="sw-star"
                    style={{ animationDelay: `${(i % 9) * 0.37}s` }}
                />
            ))}

            {/* A radar sweep round the engine while the swarm works. */}
            {running ? (
                <g className="sw-sweep">
                    <path d={`M ${CX} ${CY} L ${CX + RX + 12} ${CY - 44} A 180 180 0 0 1 ${CX + RX + 12} ${CY + 44} Z`} fill="url(#sw-sweep)" />
                </g>
            ) : null}

            {/* The orbit, cut into its six stages. */}
            <ellipse cx={CX} cy={CY} rx={RX} ry={RY} className="sw-orbit" />
            {(Object.keys(SECTOR) as StageId[]).map(stage => {
                const [a0, a1] = SECTOR[stage];
                const [x0, y0] = at(a0 + 2);
                const [x1, y1] = at(a1 - 2);
                const [lx, ly] = at((a0 + a1) / 2, 1.24);
                const empty = !crew.some(a => a.stage === stage);
                return (
                    <g key={stage} className={`sw-sector sw-${stages[stage]}${empty ? ' is-empty' : ''}`} style={{ ['--c' as string]: STAGE_COLOR[stage] }}>
                        <path d={`M ${x0} ${y0} A ${RX} ${RY} 0 0 1 ${x1} ${y1}`} className="sw-arc" />
                        <text x={lx} y={ly + 3} className="sw-stage-label">
                            {STAGE_TITLE[stage]}
                        </text>
                    </g>
                );
            })}

            {/* A spoke from each agent to the engine. */}
            {placed.map(s => {
                const [x, y] = seatPoint(s);
                const live = agents[s.id] === 'working';
                return (
                    <line
                        key={s.id}
                        x1={x}
                        y1={y}
                        x2={CX}
                        y2={CY}
                        className={`sw-spoke${live ? ' is-live' : ''}`}
                        style={live ? { stroke: STAGE_COLOR[s.stage] } : undefined}
                    />
                );
            })}

            {/* The engine every tool call goes through. */}
            <g transform={`translate(${CX} ${CY})`} className={`sw-core${busyTool ? ' is-busy' : ''}`}>
                <circle r="40" className="sw-core-halo" />
                <circle r="31" className="sw-core-ring" />
                <circle r="24" className="sw-core-disc" />
                <g transform="translate(-14 -16)">
                    <DuckleLogo size={28} />
                </g>
                <text y="22" className="sw-core-sub">
                    {busyTool ?? 'DUCKLE'}
                </text>
            </g>

            {placed.map(s => {
                const [x, y] = seatPoint(s);
                const state = agents[s.id] ?? 'idle';
                const Icon = STAGE_ICON[s.stage];
                const c = STAGE_COLOR[s.stage];
                const icon = s.r >= 9 ? Math.round(s.r * 1.05) : 0;
                // A crowded stage names only who is working right now.
                const named = s.label || state === 'working';
                const np = namePoint(s);
                return (
                    <g key={s.id} transform={`translate(${x} ${y})`} className={`sw-agent sw-${state}`} style={{ ['--c' as string]: c }}>
                        <title>{`${s.name}: ${state}`}</title>
                        <circle r={s.r * 2.1} className="sw-halo" />
                        <circle r={s.r + 5} className="sw-ring" />
                        <circle r={s.r} className="sw-orb" />
                        {icon ? <Icon x={-icon / 2} y={-icon / 2} width={icon} height={icon} strokeWidth={2.2} style={{ color: c }} /> : null}
                        {named ? (
                            <text x={np.x - x} y={np.y - y} textAnchor={np.anchor} className="sw-label">
                                {s.name.length > 14 ? `${s.name.slice(0, 13)}…` : s.name}
                            </text>
                        ) : null}
                        {state === 'done' ? (
                            <g transform={`translate(${s.r * 0.78} ${-s.r * 0.78})`}>
                                <circle r="5.5" className="sw-badge-ok" />
                                <path d="M-2.4 0 l1.7 1.8 l3.1 -3.5" className="sw-tick" />
                            </g>
                        ) : null}
                        {state === 'failed' ? (
                            <g transform={`translate(${s.r * 0.78} ${-s.r * 0.78})`}>
                                <circle r="5.5" className="sw-badge-bad" />
                                <path d="M-2 -2 l4 4 M2 -2 l-4 4" className="sw-tick" />
                            </g>
                        ) : null}
                    </g>
                );
            })}

            {particles.map(p => {
                const t = Math.min(1, Math.max(0, (now - p.born) / p.dur));
                const color = colorOf(p, byId);
                const trail = p.kind === 'handoff' ? 8 : 4;
                const size = p.kind === 'handoff' ? 4.4 : 3.6;
                return (
                    <g key={p.id} filter="url(#sw-blur)" style={{ fill: color }}>
                        {Array.from({ length: trail }, (_, k) => {
                            const pt = pointOf(p, Math.max(0, t - k * 0.034), byId);
                            if (!pt) return null;
                            const s = size * (1 - k / (trail + 1));
                            return (
                                <rect
                                    key={k}
                                    x={pt[0] - s / 2}
                                    y={pt[1] - s / 2}
                                    width={s}
                                    height={s}
                                    opacity={(1 - k / trail) * (t < 0.9 ? 1 : (1 - t) / 0.1)}
                                />
                            );
                        })}
                    </g>
                );
            })}
        </svg>
    );
}
