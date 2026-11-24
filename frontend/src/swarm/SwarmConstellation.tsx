// The swarm at a glance: the agents in orbit around the Duckle engine.
//
// The working agents glow and their orbits turn; each tool call flies to the
// core and its answer flies back (blue when it worked, red when it did not);
// a hand-off sends a comet along the orbit to the next agent. Particles are
// positioned from requestAnimationFrame, so they start the moment their event
// arrives rather than on SVG's own document clock.
//
// The crew can be any size: agents share the orbit evenly in stage order, so
// the default six sit where they always have, and a large crew gets smaller
// orbs, named only while they work.

import { useEffect, useMemo, useRef, useState } from 'react';
import type { AgentState, StageId } from './swarm-state';
import { STAGE_ICON } from './swarm-look';

const W = 460;
const H = 270;
const CX = 230;
const CY = 138;
const RX = 172;
const RY = 96;

const STAGE_ORDER: StageId[] = ['discover', 'design', 'build', 'test', 'repair', 'review'];

/** Each stage's colours, from the brand palette: orange, hot orange, slate
 *  blue and maya blue. */
export const STAGE_HUES: Record<StageId, [string, string]> = {
    discover: ['#2eafff', '#7d66ff'],
    design: ['#7d66ff', '#2eafff'],
    build: ['#ff7a45', '#ff6900'],
    test: ['#2eafff', '#2eafff'],
    repair: ['#ff6900', '#ff7a45'],
    review: ['#7d66ff', '#2eafff'],
};

type Member = { id: string; name: string; stage: StageId };

export type Seat = Member & { deg: number; scale: number; named: boolean };

/** Where each agent sits: the orbit shared evenly in stage order, from the
 *  left, clockwise. Six agents land where the first swarm view had them. */
export function seatsOf(crew: Member[]): Seat[] {
    const ordered = [...crew].sort((a, b) => STAGE_ORDER.indexOf(a.stage) - STAGE_ORDER.indexOf(b.stage));
    const n = Math.max(1, ordered.length);
    const scale = n <= 6 ? 1 : Math.max(0.5, Math.sqrt(6 / n));
    return ordered.map((m, i) => ({ ...m, deg: (180 + (i * 360) / n) % 360, scale, named: n <= 10 }));
}

function at(deg: number): [number, number] {
    const r = (deg * Math.PI) / 180;
    return [CX + RX * Math.cos(r), CY + RY * Math.sin(r)];
}

export type Flight =
    | { kind: 'call' | 'answer'; agent: string; ok?: boolean }
    | { kind: 'handoff'; from: string; to: string };

type Particle = Flight & { id: number; born: number; dur: number };

/** A point along a flight, t from 0 to 1. */
function pointOf(p: Particle, t: number, byId: Map<string, Seat>): [number, number] | null {
    if (p.kind === 'handoff') {
        const a = byId.get(p.from);
        const b = byId.get(p.to);
        if (!a || !b) return null;
        // The short way round the orbit.
        let d = b.deg - a.deg;
        if (d > 180) d -= 360;
        if (d < -180) d += 360;
        return at(a.deg + d * t);
    }
    const s = byId.get(p.agent);
    if (!s) return null;
    const [ax, ay] = at(s.deg);
    const [x0, y0, x1, y1] = p.kind === 'call' ? [ax, ay, CX, CY] : [CX, CY, ax, ay];
    // A gentle bow, outbound and return on opposite sides.
    const mx = (x0 + x1) / 2 + (y1 - y0) * 0.18;
    const my = (y0 + y1) / 2 - (x1 - x0) * 0.18;
    const u = 1 - t;
    return [u * u * x0 + 2 * u * t * mx + t * t * x1, u * u * y0 + 2 * u * t * my + t * t * y1];
}

function colorOf(p: Particle, byId: Map<string, Seat>): string {
    if (p.kind === 'answer') return p.ok === false ? '#ff4d6d' : '#2eafff';
    const s = byId.get(p.kind === 'handoff' ? p.from : p.agent);
    return s ? STAGE_HUES[s.stage][0] : '#ff7a45';
}

/** Background stars, fixed so they do not jump on re-render. */
const STARS: [number, number, number][] = Array.from({ length: 46 }, (_, i) => {
    const r = (n: number) => (((Math.sin(i * 12.9898 + n * 78.233) * 43758.5453) % 1) + 1) % 1;
    return [r(1) * W, r(2) * H, 0.4 + r(3) * 1.1];
});

export default function SwarmConstellation({
    crew,
    agents,
    busyTool,
    flights,
}: {
    crew: Member[];
    agents: Record<string, AgentState>;
    /** The tool being called right now, shown at the core. */
    busyTool: string | null;
    /** Flights to launch: each new entry (by index) starts once. */
    flights: Flight[];
}) {
    const seats = useMemo(() => seatsOf(crew), [crew]);
    const byId = useMemo(() => new Map(seats.map(s => [s.id, s])), [seats]);
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
            dur: f.kind === 'handoff' ? 1100 : 700,
        }));
        launched.current = flights.length;
        setParticles(ps => [...ps, ...fresh].slice(-48));
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
        <svg className="sw-sky" viewBox={`0 0 ${W} ${H}`} role="img" aria-label={`The agent swarm: ${crew.length} agents`}>
            <defs>
                <radialGradient id="sw-bg" cx="50%" cy="50%" r="70%">
                    <stop offset="0%" stopColor="#1a1430" />
                    <stop offset="55%" stopColor="#0c1020" />
                    <stop offset="100%" stopColor="#06080f" />
                </radialGradient>
                <radialGradient id="sw-core" cx="50%" cy="40%" r="60%">
                    <stop offset="0%" stopColor="#ffb08a" />
                    <stop offset="45%" stopColor="#ff7a45" />
                    <stop offset="100%" stopColor="#7a2a0a" />
                </radialGradient>
                <radialGradient id="sw-orb" cx="45%" cy="35%" r="70%">
                    <stop offset="0%" stopColor="#2a3350" />
                    <stop offset="100%" stopColor="#0b0f1c" />
                </radialGradient>
                {STAGE_ORDER.map(st => (
                    <radialGradient key={st} id={`sw-halo-${st}`}>
                        <stop offset="0%" stopColor={STAGE_HUES[st][0]} stopOpacity="0.55" />
                        <stop offset="100%" stopColor={STAGE_HUES[st][1]} stopOpacity="0" />
                    </radialGradient>
                ))}
                <filter id="sw-glow" x="-50%" y="-50%" width="200%" height="200%">
                    <feGaussianBlur stdDeviation="2.4" result="b" />
                    <feMerge>
                        <feMergeNode in="b" />
                        <feMergeNode in="SourceGraphic" />
                    </feMerge>
                </filter>
            </defs>

            <rect x="0" y="0" width={W} height={H} rx="14" fill="url(#sw-bg)" />
            {STARS.map(([x, y, r], i) => (
                <circle key={i} cx={x} cy={y} r={r} className="sw-star" style={{ animationDelay: `${(i % 9) * 0.4}s` }} />
            ))}

            {/* The orbit, and a spoke from each agent to the engine. */}
            <ellipse cx={CX} cy={CY} rx={RX} ry={RY} className="sw-orbit-line" />
            {seats.map(s => {
                const [x, y] = at(s.deg);
                const live = agents[s.id] === 'working';
                return (
                    <line
                        key={s.id}
                        x1={x}
                        y1={y}
                        x2={CX}
                        y2={CY}
                        className={`sw-spoke${live ? ' sw-spoke-live' : ''}`}
                        style={live ? { stroke: STAGE_HUES[s.stage][0] } : undefined}
                    />
                );
            })}

            {/* The engine every tool call goes through. */}
            <g transform={`translate(${CX} ${CY})`} className={`sw-core${busyTool ? ' sw-core-busy' : ''}`}>
                <circle r="44" className="sw-core-halo" />
                <circle r="34" className="sw-core-ring" />
                <circle r="26" fill="url(#sw-core)" filter="url(#sw-glow)" />
                <text y="-1" className="sw-core-name">
                    DUCKLE
                </text>
                <text y="10" className="sw-core-sub">
                    {busyTool ?? 'engine'}
                </text>
            </g>

            {seats.map(s => {
                const [x, y] = at(s.deg);
                const state = agents[s.id] ?? 'idle';
                const Icon = STAGE_ICON[s.stage];
                const [c0] = STAGE_HUES[s.stage];
                const k = s.scale;
                const icon = Math.round(18 * k);
                return (
                    <g key={s.id} transform={`translate(${x} ${y})`} className={`sw-agent sw-${state}`}>
                        <title>{`${s.name}: ${state}`}</title>
                        <circle r={34 * k} fill={`url(#sw-halo-${s.stage})`} className="sw-halo" />
                        <circle r={25 * k} className="sw-ring" style={{ stroke: c0 }} />
                        <circle r={19 * k} fill="url(#sw-orb)" stroke={c0} strokeWidth="2" />
                        {icon >= 9 ? <Icon x={-icon / 2} y={-icon / 2} width={icon} height={icon} color={c0} strokeWidth={2} /> : null}
                        {s.named || state === 'working' ? (
                            <text y={19 * k + 18} className="sw-name">
                                {s.name.length > 16 ? `${s.name.slice(0, 15)}…` : s.name}
                            </text>
                        ) : null}
                        {state === 'done' ? (
                            <g transform={`translate(${15 * k} ${-15 * k})`}>
                                <circle r="7" fill="#2eafff" />
                                <path d="M-3 0 l2 2.2 l4 -4.4" className="sw-tick" />
                            </g>
                        ) : null}
                        {state === 'failed' ? (
                            <g transform={`translate(${15 * k} ${-15 * k})`}>
                                <circle r="7" fill="#ff4d6d" />
                                <path d="M-2.6 -2.6 l5.2 5.2 M2.6 -2.6 l-5.2 5.2" className="sw-tick" />
                            </g>
                        ) : null}
                    </g>
                );
            })}

            {particles.map(p => {
                const t = Math.min(1, Math.max(0, (now - p.born) / p.dur));
                const color = colorOf(p, byId);
                const trail = p.kind === 'handoff' ? 7 : 4;
                return (
                    <g key={p.id} filter="url(#sw-glow)">
                        {Array.from({ length: trail }, (_, k) => {
                            const pt = pointOf(p, Math.max(0, t - k * 0.035), byId);
                            if (!pt) return null;
                            return (
                                <circle
                                    key={k}
                                    cx={pt[0]}
                                    cy={pt[1]}
                                    r={(p.kind === 'handoff' ? 4.2 : 3) * (1 - k / (trail + 1))}
                                    fill={color}
                                    opacity={(1 - k / trail) * (t < 0.92 ? 1 : (1 - t) / 0.08)}
                                />
                            );
                        })}
                    </g>
                );
            })}
        </svg>
    );
}
