// The swarm at a glance: six agents in orbit around the Duckle engine.
//
// The working agent glows and its orbit turns; each tool call it makes flies to
// the core and its answer flies back (blue when it worked, red when it did not);
// a hand-off sends a comet along the orbit to the next agent. Particles are
// positioned from requestAnimationFrame, so they start the moment their event
// arrives rather than on SVG's own document clock.

import { useEffect, useRef, useState } from 'react';
import { DraftingCompass, FlaskConical, Hammer, ScanSearch, ShieldCheck, Wrench } from 'lucide-react';
import type { AgentState, RoleId } from './swarm-state';

const W = 460;
const H = 270;
const CX = 230;
const CY = 138;
const RX = 172;
const RY = 96;

/** Where each agent sits on the orbit, in degrees (0 = right, clockwise). */
const ANGLE: Record<RoleId, number> = {
    scout: 180,
    architect: 240,
    builder: 300,
    tester: 0,
    fixer: 60,
    reviewer: 120,
};

export const ROLE_COLOR: Record<RoleId, [string, string]> = {
    scout: ['#2eafff', '#7d66ff'],
    architect: ['#7d66ff', '#b7a8ff'],
    builder: ['#ff7a45', '#ff6900'],
    tester: ['#2eafff', '#9bdcff'],
    fixer: ['#fbbf24', '#ff7a45'],
    reviewer: ['#7d66ff', '#2eafff'],
};

const ICON = {
    scout: ScanSearch,
    architect: DraftingCompass,
    builder: Hammer,
    tester: FlaskConical,
    fixer: Wrench,
    reviewer: ShieldCheck,
} as const;

const TITLE: Record<RoleId, string> = {
    scout: 'Scout',
    architect: 'Architect',
    builder: 'Builder',
    tester: 'Tester',
    fixer: 'Fixer',
    reviewer: 'Reviewer',
};

function at(deg: number): [number, number] {
    const r = (deg * Math.PI) / 180;
    return [CX + RX * Math.cos(r), CY + RY * Math.sin(r)];
}

export type Flight =
    | { kind: 'call' | 'answer'; role: RoleId; ok?: boolean }
    | { kind: 'handoff'; from: RoleId; to: RoleId };

type Particle = Flight & { id: number; born: number; dur: number };

/** A point along a flight, t from 0 to 1. */
function pointOf(p: Particle, t: number): [number, number] {
    if (p.kind === 'handoff') {
        const a = ANGLE[p.from];
        let b = ANGLE[p.to];
        // The short way round the orbit.
        let d = b - a;
        if (d > 180) d -= 360;
        if (d < -180) d += 360;
        b = a + d;
        return at(a + (b - a) * t);
    }
    const [ax, ay] = at(ANGLE[p.role]);
    const [x0, y0, x1, y1] = p.kind === 'call' ? [ax, ay, CX, CY] : [CX, CY, ax, ay];
    // A gentle bow, outbound and return on opposite sides.
    const mx = (x0 + x1) / 2 + (y1 - y0) * 0.18;
    const my = (y0 + y1) / 2 - (x1 - x0) * 0.18;
    const u = 1 - t;
    return [u * u * x0 + 2 * u * t * mx + t * t * x1, u * u * y0 + 2 * u * t * my + t * t * y1];
}

function colorOf(p: Particle): string {
    if (p.kind === 'handoff') return ROLE_COLOR[p.from][0];
    if (p.kind === 'answer') return p.ok === false ? '#ff4d6d' : '#2eafff';
    return ROLE_COLOR[p.role][0];
}

/** Background stars, fixed so they do not jump on re-render. */
const STARS: [number, number, number][] = Array.from({ length: 46 }, (_, i) => {
    const r = (n: number) => ((Math.sin(i * 12.9898 + n * 78.233) * 43758.5453) % 1 + 1) % 1;
    return [r(1) * W, r(2) * H, 0.4 + r(3) * 1.1];
});

export default function SwarmConstellation({
    agents,
    active,
    busyTool,
    flights,
}: {
    agents: Record<RoleId, AgentState>;
    active: RoleId | null;
    /** The tool being called right now, shown at the core. */
    busyTool: string | null;
    /** Flights to launch: each new entry (by index) starts once. */
    flights: Flight[];
}) {
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
        setParticles(ps => [...ps, ...fresh].slice(-40));
    }, [flights]);

    useEffect(() => {
        if (!particles.length) return;
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
    }, [particles.length > 0]); // eslint-disable-line react-hooks/exhaustive-deps

    const roles = Object.keys(ANGLE) as RoleId[];

    return (
        <svg className="sw-sky" viewBox={`0 0 ${W} ${H}`} role="img" aria-label="The agent swarm">
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
                {roles.map(r => (
                    <radialGradient key={r} id={`sw-halo-${r}`}>
                        <stop offset="0%" stopColor={ROLE_COLOR[r][0]} stopOpacity="0.55" />
                        <stop offset="100%" stopColor={ROLE_COLOR[r][1]} stopOpacity="0" />
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
            {roles.map(r => {
                const [x, y] = at(ANGLE[r]);
                return (
                    <line
                        key={r}
                        x1={x}
                        y1={y}
                        x2={CX}
                        y2={CY}
                        className={`sw-spoke${active === r ? ' sw-spoke-live' : ''}`}
                        style={active === r ? { stroke: ROLE_COLOR[r][0] } : undefined}
                    />
                );
            })}

            {/* The engine every tool call goes through. */}
            <g transform={`translate(${CX} ${CY})`} className={`sw-core${busyTool ? ' sw-core-busy' : ''}`}>
                <circle r="44" className="sw-core-halo" />
                <circle r="34" className="sw-core-ring" />
                <circle r="26" fill="url(#sw-core)" filter="url(#sw-glow)" />
                <text y="-1" className="sw-core-name">DUCKLE</text>
                <text y="10" className="sw-core-sub">
                    {busyTool ?? 'engine'}
                </text>
            </g>

            {roles.map(r => {
                const [x, y] = at(ANGLE[r]);
                const state = agents[r];
                const Icon = ICON[r];
                const [c0] = ROLE_COLOR[r];
                return (
                    <g key={r} transform={`translate(${x} ${y})`} className={`sw-agent sw-${state}`}>
                        <circle r="34" fill={`url(#sw-halo-${r})`} className="sw-halo" />
                        <circle r="25" className="sw-ring" style={{ stroke: c0 }} />
                        <circle r="19" fill="url(#sw-orb)" stroke={c0} strokeWidth="2" />
                        <Icon x={-9} y={-9} width={18} height={18} color={c0} strokeWidth={2} />
                        <text y="37" className="sw-label">
                            {TITLE[r]}
                        </text>
                        {state === 'done' ? (
                            <g transform="translate(15 -15)">
                                <circle r="7" fill="#2eafff" />
                                <path d="M-3 0 l2 2.2 l4 -4.4" className="sw-tick" />
                            </g>
                        ) : null}
                        {state === 'failed' ? (
                            <g transform="translate(15 -15)">
                                <circle r="7" fill="#ff4d6d" />
                                <path d="M-2.6 -2.6 l5.2 5.2 M2.6 -2.6 l-5.2 5.2" className="sw-tick" />
                            </g>
                        ) : null}
                    </g>
                );
            })}

            {particles.map(p => {
                const t = Math.min(1, Math.max(0, (now - p.born) / p.dur));
                const color = colorOf(p);
                const trail = p.kind === 'handoff' ? 7 : 4;
                return (
                    <g key={p.id} filter="url(#sw-glow)">
                        {Array.from({ length: trail }, (_, k) => {
                            const tk = Math.max(0, t - k * 0.035);
                            const [x, y] = pointOf(p, tk);
                            return (
                                <circle
                                    key={k}
                                    cx={x}
                                    cy={y}
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
