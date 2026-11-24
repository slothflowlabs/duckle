// The swarm, alive: every agent is a cloud of glowing pixels around the
// Duckle engine.
//
// An idle cloud drifts; a working one flares, spins faster and streams energy
// to the engine. Each tool call flies to the engine as a burst of pixels and
// its answer flies back (the success colour, or danger when it failed); a
// hand-off travels between clouds as a comet ribbon, one to each agent of a
// stage that fans out. Motion leaves trails. A passed mission ends in a
// shockwave. Nothing is labelled but the agents at work, and whoever the
// pointer rests on.
//
// It is drawn on a canvas from requestAnimationFrame, so nothing in React
// re-renders per frame: the loop reads the latest props from a ref. Colours
// are read from the theme's tokens, and read again when the theme changes.

import { useEffect, useRef } from 'react';
import type { AgentState, StageId, StageState } from './swarm-state';
import { STAGE_COLOR } from './swarm-look';

export type Flight =
    | { kind: 'call' | 'answer'; agent: string; ok?: boolean }
    | { kind: 'handoff'; from: string; to: string };

type Member = { id: string; name: string; stage: StageId };

type Props = {
    crew: Member[];
    agents: Record<string, AgentState>;
    stages: Record<StageId, StageState>;
    running: boolean;
    /** The tool being called right now, shown under the engine. */
    busyTool: string | null;
    /** Flights to launch: each new entry (by index) starts once. */
    flights: Flight[];
    /** How the last run ended, for the finale. */
    outcome: 'pass' | 'fail' | null;
};

const TAU = Math.PI * 2;

/** Each stage's arc round the engine, in degrees (0 = right, clockwise): the
 *  swarm reads left to right in pipeline order, without drawing a diagram. */
const SECTOR: Record<StageId, [number, number]> = {
    discover: [150, 210],
    design: [210, 270],
    build: [270, 330],
    test: [330, 390],
    repair: [30, 90],
    review: [90, 150],
};

export type Seat = { id: string; stage: StageId; x: number; y: number; scale: number };

/** Where each agent's cloud rests in a field w x h: spread across its stage's
 *  arc, crowded stages on two rings and smaller. Pure, so it can be checked. */
export function layout(crew: Member[], w: number, h: number): Seat[] {
    const cx = w / 2;
    const cy = h / 2;
    const rx = w * 0.36;
    const ry = h * 0.33;
    const out: Seat[] = [];
    for (const stage of Object.keys(SECTOR) as StageId[]) {
        const members = crew.filter(a => a.stage === stage);
        const k = members.length;
        if (!k) continue;
        const [a0, a1] = SECTOR[stage];
        const scale = k <= 2 ? 1 : Math.max(0.42, 1.7 / Math.sqrt(k));
        members.forEach((m, i) => {
            const deg = a0 + ((i + 0.5) * (a1 - a0)) / k;
            const ring = k > 4 && i % 2 ? 0.7 : 1;
            const r = (deg * Math.PI) / 180;
            out.push({ id: m.id, stage, x: cx + rx * ring * Math.cos(r), y: cy + ry * ring * Math.sin(r), scale });
        });
    }
    return out;
}

type RGB = [number, number, number];
type Mote = { a: number; r: number; v: number; s: number; ph: number };
type End = string; // an agent id, or 'core'
type Packet = { from: End; to: End; t: number; dur: number; col: RGB; size: number; bend: number; arc: boolean };
type Ring = { x: number; y: number; t: number; dur: number; col: RGB; max: number; width: number };
type Dust = { x: number; y: number; vx: number; vy: number; s: number; a: number };

/** A stable number in [0, 1) for a string. */
function hash(s: string): number {
    let h = 2166136261;
    for (let i = 0; i < s.length; i++) {
        h ^= s.charCodeAt(i);
        h = Math.imul(h, 16777619);
    }
    return (h >>> 0) / 4294967296;
}

function rgba([r, g, b]: RGB, a: number): string {
    return `rgba(${r | 0},${g | 0},${b | 0},${Math.max(0, Math.min(1, a))})`;
}

function quad(x0: number, y0: number, cx: number, cy: number, x1: number, y1: number, t: number): [number, number] {
    const u = 1 - t;
    return [u * u * x0 + 2 * u * t * cx + t * t * x1, u * u * y0 + 2 * u * t * cy + t * t * y1];
}

/** The Duckle mark (DuckleLogo.tsx), drawn at (x, y) with side `size`. */
function drawLogo(ctx: CanvasRenderingContext2D, x: number, y: number, size: number, glow: number) {
    const s = size / 64;
    ctx.save();
    ctx.translate(x - 32 * s, y - 32 * s);
    ctx.scale(s, s);
    ctx.shadowColor = 'rgba(234,126,66,0.9)';
    ctx.shadowBlur = 14 + glow * 22;
    ctx.strokeStyle = '#EA7E42';
    ctx.lineWidth = 3.4;
    ctx.lineCap = 'round';
    ctx.beginPath();
    ctx.moveTo(14, 16);
    ctx.lineTo(32, 32);
    ctx.lineTo(50, 48);
    ctx.stroke();
    const tile = (tx: number, ty: number, fill: string) => {
        ctx.fillStyle = fill;
        ctx.beginPath();
        if (ctx.roundRect) ctx.roundRect(tx, ty, 18, 18, 5.5);
        else ctx.rect(tx, ty, 18, 18);
        ctx.fill();
    };
    tile(5, 7, '#F6BA78');
    tile(23, 23, '#EA7E42');
    tile(41, 39, '#D9742F');
    ctx.restore();
}

class Field {
    private ctx: CanvasRenderingContext2D;
    private w = 0;
    private h = 0;
    private t = 0;
    private dark = true;
    private col: Record<string, RGB> = {};
    private motes = new Map<string, Mote[]>();
    private energy = new Map<string, number>();
    private last = new Map<string, AgentState>();
    private packets: Packet[] = [];
    private rings: Ring[] = [];
    private dust: Dust[] = [];
    private seats = new Map<string, Seat>();
    private seatKey = '';
    private celebrate = 0;
    private coreKick = 0;
    private outcome: 'pass' | 'fail' | null = null;
    private busy: string | null = null;
    hover: { x: number; y: number } | null = null;

    constructor(
        private canvas: HTMLCanvasElement,
        private probe: HTMLElement,
    ) {
        this.ctx = canvas.getContext('2d')!;
        this.readColors();
    }

    readColors() {
        const read = (css: string): RGB => {
            this.probe.style.color = css;
            const m = getComputedStyle(this.probe).color.match(/[\d.]+/g);
            return m ? [Number(m[0]), Number(m[1]), Number(m[2])] : [255, 122, 69];
        };
        for (const stage of Object.keys(STAGE_COLOR) as StageId[]) this.col[stage] = read(STAGE_COLOR[stage]);
        this.col.accent = read('var(--accent)');
        this.col.success = read('var(--success)');
        this.col.danger = read('var(--danger)');
        this.col.bg = read('var(--bg-0)');
        this.col.text = read('var(--text-1)');
        this.col.dim = read('var(--text-3)');
        const [r, g, b] = this.col.bg;
        this.dark = 0.2126 * r + 0.7152 * g + 0.0722 * b < 128;
        this.wipe();
    }

    resize(w: number, h: number) {
        const dpr = Math.min(2, window.devicePixelRatio || 1);
        this.w = w;
        this.h = h;
        this.canvas.width = Math.round(w * dpr);
        this.canvas.height = Math.round(h * dpr);
        this.canvas.style.width = `${w}px`;
        this.canvas.style.height = `${h}px`;
        this.ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
        this.seatKey = '';
        this.dust = Array.from({ length: Math.round((w * h) / 1600) }, (_, i) => ({
            x: hash(`dx${i}`) * w,
            y: hash(`dy${i}`) * h,
            vx: (hash(`vx${i}`) - 0.5) * 6,
            vy: (hash(`vy${i}`) - 0.5) * 4,
            s: 0.8 + hash(`ds${i}`) * 1.2,
            a: 0.12 + hash(`da${i}`) * 0.3,
        }));
        this.wipe();
    }

    private wipe() {
        if (!this.w) return;
        this.ctx.globalCompositeOperation = 'source-over';
        this.ctx.fillStyle = rgba(this.col.bg ?? [7, 9, 15], 1);
        this.ctx.fillRect(0, 0, this.w, this.h);
    }

    private moteCount(n: number): number {
        return Math.max(8, Math.min(26, Math.floor(900 / Math.max(1, n))));
    }

    private place(crew: Member[]) {
        const key = `${this.w}x${this.h}:${crew.map(m => `${m.id}/${m.stage}`).join(',')}`;
        if (key === this.seatKey) return;
        this.seatKey = key;
        this.seats = new Map(layout(crew, this.w, this.h).map(s => [s.id, s]));
        const per = this.moteCount(crew.length);
        for (const m of crew) {
            const have = this.motes.get(m.id);
            if (have && have.length === per) continue;
            const seed = hash(m.id);
            this.motes.set(
                m.id,
                Array.from({ length: per }, (_, i) => ({
                    a: hash(`${m.id}a${i}`) * TAU,
                    r: 6 + hash(`${m.id}r${i}`) * 15,
                    v: (0.5 + hash(`${m.id}v${i}`) * 1.3) * (seed > 0.5 ? 1 : -1),
                    s: 1.2 + hash(`${m.id}s${i}`) * 1.8,
                    ph: hash(`${m.id}p${i}`) * TAU,
                })),
            );
        }
    }

    /** Where an agent's cloud is right now: its seat, drifting. */
    private where(id: End): [number, number] {
        if (id === 'core') return [this.w / 2, this.h / 2];
        const s = this.seats.get(id);
        if (!s) return [this.w / 2, this.h / 2];
        const k = hash(id) * TAU;
        return [s.x + Math.sin(this.t * 0.6 + k) * 5, s.y + Math.cos(this.t * 0.45 + k * 1.7) * 4];
    }

    private stageCol(id: string, crew: Member[]): RGB {
        const m = crew.find(c => c.id === id);
        return (m && this.col[m.stage]) || this.col.accent;
    }

    launch(f: Flight, crew: Member[]) {
        if (f.kind === 'handoff') {
            const col = this.stageCol(f.from, crew);
            for (let i = 0; i < 18; i++) {
                this.packets.push({ from: f.from, to: f.to, t: -i * 0.035, dur: 1.0, col, size: 2.6 - i * 0.07, bend: 0.55, arc: true });
            }
            return;
        }
        const col = f.kind === 'call' ? this.stageCol(f.agent, crew) : f.ok === false ? this.col.danger : this.col.success;
        const [from, to] = f.kind === 'call' ? [f.agent, 'core'] : ['core', f.agent];
        for (let i = 0; i < 7; i++) {
            this.packets.push({
                from,
                to,
                t: -i * 0.04,
                dur: 0.62 + hash(`${from}${to}${this.t}${i}`) * 0.18,
                col,
                size: 2.4,
                bend: (hash(`${this.t}b${i}`) - 0.5) * 0.7,
                arc: false,
            });
        }
        if (f.kind === 'call') this.coreKick = 1;
    }

    frame(dt: number, p: Props) {
        const { ctx, w, h } = this;
        if (!w || !h) return;
        this.t += dt;
        this.busy = p.busyTool;
        this.place(p.crew);

        // Agents arriving at done or failed send out a ring of their own.
        for (const m of p.crew) {
            const now = p.agents[m.id] ?? 'idle';
            const before = this.last.get(m.id);
            if (before !== now && (now === 'done' || now === 'failed')) {
                const [x, y] = this.where(m.id);
                this.rings.push({ x, y, t: 0, dur: 0.9, col: now === 'failed' ? this.col.danger : this.stageCol(m.id, p.crew), max: 30, width: 1.6 });
            }
            this.last.set(m.id, now);
        }
        if (p.outcome !== this.outcome) {
            this.outcome = p.outcome;
            if (p.outcome === 'pass') {
                this.celebrate = 1.6;
                for (let i = 0; i < 3; i++) {
                    this.rings.push({ x: w / 2, y: h / 2, t: -i * 0.22, dur: 1.5, col: i === 1 ? this.col.success : this.col.accent, max: Math.max(w, h) * 0.7, width: 2.2 });
                }
            } else if (p.outcome === 'fail') {
                this.rings.push({ x: w / 2, y: h / 2, t: 0, dur: 1.3, col: this.col.danger, max: Math.max(w, h) * 0.6, width: 2 });
            }
        }
        this.celebrate = Math.max(0, this.celebrate - dt);
        this.coreKick = Math.max(0, this.coreKick - dt * 2.5);

        // Fade the last frame instead of clearing it: motion leaves trails.
        ctx.globalCompositeOperation = 'source-over';
        ctx.fillStyle = rgba(this.col.bg, this.dark ? 0.24 : 0.32);
        ctx.fillRect(0, 0, w, h);
        const glow: GlobalCompositeOperation = this.dark ? 'lighter' : 'source-over';

        // Dust, drifting.
        ctx.globalCompositeOperation = 'source-over';
        for (const d of this.dust) {
            d.x = (d.x + d.vx * dt + w) % w;
            d.y = (d.y + d.vy * dt + h) % h;
            ctx.fillStyle = rgba(this.col.dim, d.a * (0.6 + 0.4 * Math.sin(this.t * 1.3 + d.x)));
            ctx.fillRect(d.x, d.y, d.s, d.s);
        }

        const cx = w / 2;
        const cy = h / 2;
        ctx.globalCompositeOperation = glow;

        // The engine's glow, and its two counter-turning rings.
        const busy = !!this.busy;
        const pulse = 0.5 + 0.5 * Math.sin(this.t * (busy ? 7 : 2.2));
        const halo = ctx.createRadialGradient(cx, cy, 0, cx, cy, 58 + this.coreKick * 18);
        halo.addColorStop(0, rgba(this.col.accent, 0.28 + 0.18 * pulse + this.coreKick * 0.25));
        halo.addColorStop(1, rgba(this.col.accent, 0));
        ctx.fillStyle = halo;
        ctx.beginPath();
        ctx.arc(cx, cy, 58 + this.coreKick * 18, 0, TAU);
        ctx.fill();
        const spin = this.t * (busy ? 2.4 : 0.6);
        ctx.lineWidth = 1.3;
        ctx.setLineDash([3, 6]);
        ctx.strokeStyle = rgba(this.col.accent, 0.55);
        ctx.beginPath();
        ctx.arc(cx, cy, 27, spin, spin + TAU);
        ctx.stroke();
        ctx.setLineDash([10, 14]);
        ctx.strokeStyle = rgba(this.col.accent, 0.3);
        ctx.beginPath();
        ctx.arc(cx, cy, 36, -spin * 0.7, -spin * 0.7 + TAU);
        ctx.stroke();
        ctx.setLineDash([]);

        // Clouds: one per agent.
        for (const m of p.crew) {
            const state = p.agents[m.id] ?? 'idle';
            const target =
                this.celebrate > 0
                    ? 0.9
                    : state === 'working'
                      ? 1
                      : state === 'failed'
                        ? 0.55
                        : state === 'done'
                          ? 0.42
                          : p.running
                            ? 0.2
                            : 0.5;
            const e0 = this.energy.get(m.id) ?? target;
            const e = e0 + (target - e0) * Math.min(1, dt * 3);
            this.energy.set(m.id, e);
            const [ax, ay] = this.where(m.id);
            const seat = this.seats.get(m.id);
            const scale = seat?.scale ?? 1;
            const col = state === 'failed' ? this.col.danger : this.stageCol(m.id, p.crew);

            if (state === 'working') {
                // A beam of energy to the engine, with pixels running along it.
                const bx = (ax + cx) / 2 + (cy - ay) * 0.12;
                const by = (ay + cy) / 2 - (cx - ax) * 0.12;
                ctx.strokeStyle = rgba(col, 0.22);
                ctx.lineWidth = 1.2;
                ctx.beginPath();
                ctx.moveTo(ax, ay);
                ctx.quadraticCurveTo(bx, by, cx, cy);
                ctx.stroke();
                for (let i = 0; i < 4; i++) {
                    const tt = (this.t * 0.9 + i / 4 + hash(m.id)) % 1;
                    const [px, py] = quad(ax, ay, bx, by, cx, cy, tt);
                    ctx.fillStyle = rgba(col, 0.85 * Math.sin(tt * Math.PI));
                    ctx.fillRect(px - 1.2, py - 1.2, 2.4, 2.4);
                }
                const aura = ctx.createRadialGradient(ax, ay, 0, ax, ay, 34 * scale);
                aura.addColorStop(0, rgba(col, 0.32));
                aura.addColorStop(1, rgba(col, 0));
                ctx.fillStyle = aura;
                ctx.beginPath();
                ctx.arc(ax, ay, 34 * scale, 0, TAU);
                ctx.fill();
            }
            // Every cloud keeps a soft glow of its own, so it reads as one being.
            const soft = ctx.createRadialGradient(ax, ay, 0, ax, ay, 24 * scale);
            soft.addColorStop(0, rgba(col, 0.1 + e * 0.12));
            soft.addColorStop(1, rgba(col, 0));
            ctx.fillStyle = soft;
            ctx.beginPath();
            ctx.arc(ax, ay, 24 * scale, 0, TAU);
            ctx.fill();
            const jitter = state === 'failed' ? 1.6 : 0;
            for (const mo of this.motes.get(m.id) ?? []) {
                mo.a += mo.v * dt * (0.5 + e * 2.6);
                const rr = mo.r * scale * (0.5 + e * 0.95) * (1 + 0.14 * Math.sin(this.t * 2 + mo.ph));
                const x = ax + Math.cos(mo.a) * rr + (jitter ? (Math.random() - 0.5) * jitter : 0);
                const y = ay + Math.sin(mo.a) * rr * 0.78 + (jitter ? (Math.random() - 0.5) * jitter : 0);
                const s = mo.s * (0.7 + e * 0.7) * Math.max(0.6, scale);
                ctx.fillStyle = rgba(col, 0.22 + e * 0.72);
                ctx.fillRect(x - s / 2, y - s / 2, s, s);
            }
            if (state === 'done' || state === 'failed') {
                ctx.strokeStyle = rgba(col, 0.45);
                ctx.lineWidth = 1;
                ctx.beginPath();
                ctx.arc(ax, ay, 22 * scale, 0, TAU);
                ctx.stroke();
            }
        }

        // Pixels in flight: calls, answers and hand-offs.
        this.packets = this.packets.filter(pk => pk.t < pk.dur);
        for (const pk of this.packets) {
            pk.t += dt;
            if (pk.t < 0) continue;
            const k = Math.min(1, pk.t / pk.dur);
            const ease = k < 0.5 ? 2 * k * k : 1 - Math.pow(-2 * k + 2, 2) / 2;
            const [x0, y0] = this.where(pk.from);
            const [x1, y1] = this.where(pk.to);
            let bx: number;
            let by: number;
            if (pk.arc) {
                // A hand-off arcs outward, away from the engine.
                const mx = (x0 + x1) / 2;
                const my = (y0 + y1) / 2;
                bx = mx + (mx - cx) * pk.bend;
                by = my + (my - cy) * pk.bend;
            } else {
                bx = (x0 + x1) / 2 + (y1 - y0) * pk.bend;
                by = (y0 + y1) / 2 - (x1 - x0) * pk.bend;
            }
            const [x, y] = quad(x0, y0, bx, by, x1, y1, ease);
            const fade = k > 0.85 ? (1 - k) / 0.15 : 1;
            ctx.fillStyle = rgba(pk.col, 0.95 * fade);
            ctx.fillRect(x - pk.size / 2, y - pk.size / 2, pk.size, pk.size);
        }

        // Shockwaves.
        this.rings = this.rings.filter(r => r.t < r.dur);
        for (const r of this.rings) {
            r.t += dt;
            if (r.t < 0) continue;
            const k = r.t / r.dur;
            ctx.strokeStyle = rgba(r.col, (1 - k) * 0.85);
            ctx.lineWidth = r.width * (1 - k * 0.6);
            ctx.beginPath();
            ctx.arc(r.x, r.y, 4 + r.max * (1 - Math.pow(1 - k, 3)), 0, TAU);
            ctx.stroke();
        }

        // The engine itself, on top.
        ctx.globalCompositeOperation = 'source-over';
        ctx.fillStyle = rgba(this.col.bg, 0.92);
        ctx.beginPath();
        ctx.arc(cx, cy, 20, 0, TAU);
        ctx.fill();
        drawLogo(ctx, cx, cy, 26, pulse * 0.6 + this.coreKick);
        if (this.busy) {
            ctx.font = '600 8.5px ui-monospace, Consolas, monospace';
            ctx.textAlign = 'center';
            ctx.fillStyle = rgba(this.col.text, 0.8);
            ctx.fillText(this.busy, cx, cy + 50);
        }

        // Names: the agents at work, and whoever the pointer rests on. A tag
        // sits above its cloud, below it when that would meet the read-out
        // along the top, and steps aside from any tag already placed.
        const hovered = this.hover ? this.nearest(this.hover.x, this.hover.y) : null;
        const placed: { l: number; t: number; r: number; b: number }[] = [];
        ctx.font = '700 10px Inter, system-ui, sans-serif';
        for (const m of p.crew) {
            const state = p.agents[m.id] ?? 'idle';
            if (state !== 'working' && m.id !== hovered) continue;
            const [ax, ay] = this.where(m.id);
            const label = m.id === hovered && state !== 'working' ? `${m.name} · ${state}` : m.name;
            const reach = 26 * (this.seats.get(m.id)?.scale ?? 1) + 6;
            const pw = ctx.measureText(label).width + 14;
            const left = Math.max(4, Math.min(w - pw - 4, ax - pw / 2));
            let top = ay - reach - 9;
            const below = top < 28;
            if (below) top = ay + reach - 9;
            for (let tries = 0; tries < 6; tries++) {
                const hit = placed.some(q => left < q.r && left + pw > q.l && top < q.b && top + 18 > q.t);
                if (!hit) break;
                top += below ? 21 : -21;
                if (top < 28) {
                    top = ay + reach - 9 + 21 * (tries + 1);
                }
            }
            top = Math.max(28, Math.min(h - 22, top));
            placed.push({ l: left, t: top, r: left + pw, b: top + 18 });
            this.tag(label, left, top, pw, this.stageCol(m.id, p.crew));
        }
    }

    private tag(text: string, left: number, top: number, pw: number, col: RGB) {
        const { ctx } = this;
        ctx.font = '700 10px Inter, system-ui, sans-serif';
        ctx.fillStyle = rgba(this.col.bg, 0.82);
        ctx.strokeStyle = rgba(col, 0.75);
        ctx.lineWidth = 1;
        ctx.beginPath();
        if (ctx.roundRect) ctx.roundRect(left, top, pw, 18, 9);
        else ctx.rect(left, top, pw, 18);
        ctx.fill();
        ctx.stroke();
        ctx.fillStyle = rgba(col, 1);
        ctx.textAlign = 'left';
        ctx.fillText(text, left + 7, top + 12.5);
    }

    private nearest(x: number, y: number): string | null {
        let best: string | null = null;
        let dist = 26;
        for (const id of this.seats.keys()) {
            const [ax, ay] = this.where(id);
            const d = Math.hypot(ax - x, ay - y);
            if (d < dist) {
                dist = d;
                best = id;
            }
        }
        return best;
    }
}

export default function SwarmField(props: Props) {
    const wrap = useRef<HTMLDivElement | null>(null);
    const canvas = useRef<HTMLCanvasElement | null>(null);
    const probe = useRef<HTMLSpanElement | null>(null);
    const latest = useRef(props);
    latest.current = props;
    const field = useRef<Field | null>(null);
    const launched = useRef(0);

    useEffect(() => {
        if (!canvas.current || !wrap.current || !probe.current) return;
        const f = new Field(canvas.current, probe.current);
        field.current = f;
        const fit = () => {
            const r = wrap.current!.getBoundingClientRect();
            f.resize(Math.max(1, Math.round(r.width)), Math.max(1, Math.round(r.height)));
        };
        fit();
        const ro = new ResizeObserver(fit);
        ro.observe(wrap.current);
        // The theme changes the tokens: read them again.
        const mo = new MutationObserver(() => f.readColors());
        mo.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme', 'class'] });
        const still = window.matchMedia?.('(prefers-reduced-motion: reduce)').matches;
        let raf = 0;
        let prev = performance.now();
        const loop = (now: number) => {
            const dt = Math.min(0.05, (now - prev) / 1000);
            prev = now;
            f.frame(still ? dt * 0.25 : dt, latest.current);
            raf = requestAnimationFrame(loop);
        };
        raf = requestAnimationFrame(loop);
        return () => {
            cancelAnimationFrame(raf);
            ro.disconnect();
            mo.disconnect();
            field.current = null;
        };
    }, []);

    useEffect(() => {
        const f = field.current;
        if (!f) return;
        if (props.flights.length < launched.current) launched.current = 0;
        for (const fl of props.flights.slice(launched.current)) f.launch(fl, props.crew);
        launched.current = props.flights.length;
    }, [props.flights, props.crew]);

    return (
        <div
            className="sw-field"
            ref={wrap}
            onMouseMove={e => {
                const r = e.currentTarget.getBoundingClientRect();
                if (field.current) field.current.hover = { x: e.clientX - r.left, y: e.clientY - r.top };
            }}
            onMouseLeave={() => {
                if (field.current) field.current.hover = null;
            }}
        >
            <canvas ref={canvas} role="img" aria-label={`The agent swarm: ${props.crew.length} agents`} />
            <span ref={probe} className="sw-probe" aria-hidden="true" />
        </div>
    );
}
