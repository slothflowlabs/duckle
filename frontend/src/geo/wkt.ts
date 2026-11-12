// #372: read a GEOMETRY value the way DuckDB prints it in a preview row.
//
// DuckDB 2.0 has GEOMETRY as a core type and prints it as WKT, in -json output
// and in COPY ... (FORMAT json) alike, so every preview surface hands the map
// text like "POINT (1.5 2)" or "MULTIPOINT (1 2, 3 4)". Only x and y are kept: Z
// and M values have nothing to be drawn against on a flat map.

/** One coordinate, x then y (longitude then latitude for a geographic CRS). */
export type Position = [number, number];

export type Geometry =
    | { type: 'Point'; coordinates: Position }
    | { type: 'LineString'; coordinates: Position[] }
    | { type: 'Polygon'; coordinates: Position[][] }
    | { type: 'MultiPoint'; coordinates: Position[] }
    | { type: 'MultiLineString'; coordinates: Position[][] }
    | { type: 'MultiPolygon'; coordinates: Position[][][] }
    | { type: 'GeometryCollection'; geometries: Geometry[] };

type Token = { kind: 'word' | 'num' | '(' | ')' | ','; text: string };

const NUMBER = /[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?/y;
const WORD = /[A-Za-z]+/y;

function tokenize(text: string): Token[] | null {
    const out: Token[] = [];
    let i = 0;
    while (i < text.length) {
        const c = text[i];
        if (c === ' ' || c === '\t' || c === '\n' || c === '\r') {
            i++;
        } else if (c === '(' || c === ')' || c === ',') {
            out.push({ kind: c, text: c });
            i++;
        } else {
            NUMBER.lastIndex = i;
            WORD.lastIndex = i;
            const num = NUMBER.exec(text);
            const word = num ? null : WORD.exec(text);
            const hit = num ?? word;
            if (!hit) return null;
            out.push({ kind: num ? 'num' : 'word', text: num ? hit[0] : hit[0].toUpperCase() });
            i += hit[0].length;
        }
    }
    return out;
}

const EMPTY: Geometry = { type: 'GeometryCollection', geometries: [] };

class Reader {
    private at = 0;
    constructor(private readonly tokens: Token[]) {}
    peek(): Token | undefined {
        return this.tokens[this.at];
    }
    take(kind: Token['kind'], text?: string): boolean {
        const t = this.tokens[this.at];
        if (!t || t.kind !== kind || (text !== undefined && t.text !== text)) return false;
        this.at++;
        return true;
    }
    word(): string | null {
        const t = this.tokens[this.at];
        if (!t || t.kind !== 'word') return null;
        this.at++;
        return t.text;
    }
    done(): boolean {
        return this.at === this.tokens.length;
    }
    /** x y, with an optional z and m that are read and dropped. */
    position(): Position | null {
        const values: number[] = [];
        while (values.length < 4 && this.peek()?.kind === 'num') {
            values.push(Number(this.tokens[this.at++].text));
        }
        if (values.length < 2 || !values.every(Number.isFinite)) return null;
        return [values[0], values[1]];
    }
    /** `( item, item, ... )`, skipping members written as EMPTY. */
    list<T>(item: () => T | null): T[] | null {
        if (!this.take('(')) return null;
        const out: T[] = [];
        do {
            if (this.take('word', 'EMPTY')) continue;
            const one = item();
            if (one === null) return null;
            out.push(one);
        } while (this.take(','));
        return this.take(')') ? out : null;
    }
    positions(): Position[] | null {
        return this.list(() => this.position());
    }
    rings(): Position[][] | null {
        return this.list(() => this.positions());
    }
    /** A MULTIPOINT member, which DuckDB writes bare and others in parentheses. */
    point(): Position | null {
        if (!this.take('(')) return this.position();
        const p = this.position();
        return p && this.take(')') ? p : null;
    }
    geometry(): Geometry | null {
        const tag = this.word();
        if (tag === null) return null;
        const dims = this.peek();
        if (dims?.kind === 'word' && (dims.text === 'Z' || dims.text === 'M' || dims.text === 'ZM')) this.at++;
        if (this.take('word', 'EMPTY')) return KNOWN.has(tag) ? EMPTY : null;
        switch (tag) {
            case 'POINT': {
                if (!this.take('(')) return null;
                const p = this.position();
                return p && this.take(')') ? { type: 'Point', coordinates: p } : null;
            }
            case 'LINESTRING': {
                const c = this.positions();
                return c && { type: 'LineString', coordinates: c };
            }
            case 'POLYGON': {
                const c = this.rings();
                return c && { type: 'Polygon', coordinates: c };
            }
            case 'MULTIPOINT': {
                const c = this.list(() => this.point());
                return c && { type: 'MultiPoint', coordinates: c };
            }
            case 'MULTILINESTRING': {
                const c = this.rings();
                return c && { type: 'MultiLineString', coordinates: c };
            }
            case 'MULTIPOLYGON': {
                const c = this.list(() => this.rings());
                return c && { type: 'MultiPolygon', coordinates: c };
            }
            case 'GEOMETRYCOLLECTION': {
                const c = this.list(() => this.geometry());
                return c && { type: 'GeometryCollection', geometries: c };
            }
            default:
                return null;
        }
    }
}

const KNOWN = new Set([
    'POINT',
    'LINESTRING',
    'POLYGON',
    'MULTIPOINT',
    'MULTILINESTRING',
    'MULTIPOLYGON',
    'GEOMETRYCOLLECTION',
]);

/**
 * Read one WKT value. `null` for anything that is not a whole shape - a masked
 * value, a number, a typo, text left over after the shape - so the caller can
 * count it rather than draw a guess. An EMPTY shape reads as a collection with
 * no members.
 */
export function parseWkt(text: unknown): Geometry | null {
    if (typeof text !== 'string') return null;
    const tokens = tokenize(text);
    if (!tokens || tokens.length === 0) return null;
    const reader = new Reader(tokens);
    const g = reader.geometry();
    return g && reader.done() ? g : null;
}
