// #372: the arithmetic behind the map of a preview's GEOMETRY column.
//
// No basemap is drawn: tiles would come from a third-party server, and nothing
// in a preview should reach the network. So the shapes alone have to come out
// the right way up, the right shape and under the cursor where they belong,
// which is all this file does. Kept free of React so check-logic can run it.

import { parseWkt, type Geometry, type Position } from './wkt';

export type Bounds = { minX: number; minY: number; maxX: number; maxY: number };
/** A drawable shape and the preview row it came from (its index in `rows`). */
export type MapFeature = { row: number; geometry: Geometry };
export type FeatureSet = {
    features: MapFeature[];
    /** Rows whose value was NULL. */
    nulls: number;
    /** Rows holding an EMPTY shape: read fine, nothing to draw. */
    empty: number;
    /** Rows whose value was not WKT, e.g. masked. */
    unreadable: number;
    /** Rows not looked at because the feature limit was reached. */
    capped: number;
};
/**
 * Map to screen: x' = x * scale * xStretch + tx, y' = -y * scale + ty (north up).
 * `xStretch` narrows longitude by cos(latitude), so a degree east is drawn as
 * long as it is on the ground at the middle of the data.
 */
export type View = { scale: number; xStretch: number; tx: number; ty: number };
/** SVG path data per layer, since points, lines and areas are drawn differently. */
export type PathParts = { points: string; lines: string; polygons: string };

/** The columns a map can be drawn from: those typed GEOMETRY. */
export function geometryColumns(columns: { name: string; type: string }[]): string[] {
    return columns.filter(c => String(c.type).toLowerCase() === 'geometry').map(c => c.name);
}

/** The CRS a DuckDB type names, as in `GEOMETRY('EPSG:4326')`; null when it names none. */
export function crsOfType(sqlType: string | undefined): string | null {
    if (!sqlType) return null;
    const m = /^\s*GEOMETRY\s*\(\s*'([^']*)'\s*\)\s*$/i.exec(sqlType);
    return m ? m[1] : null;
}

function eachPosition(g: Geometry, visit: (p: Position) => void): void {
    switch (g.type) {
        case 'Point':
            visit(g.coordinates);
            break;
        case 'LineString':
        case 'MultiPoint':
            g.coordinates.forEach(visit);
            break;
        case 'Polygon':
        case 'MultiLineString':
            g.coordinates.forEach(ring => ring.forEach(visit));
            break;
        case 'MultiPolygon':
            g.coordinates.forEach(poly => poly.forEach(ring => ring.forEach(visit)));
            break;
        case 'GeometryCollection':
            g.geometries.forEach(member => eachPosition(member, visit));
            break;
    }
}

function hasPositions(g: Geometry): boolean {
    let any = false;
    eachPosition(g, () => {
        any = true;
    });
    return any;
}

/**
 * Read the shapes in `column`, in row order, until `limit` have been found.
 * Nothing is dropped silently: a NULL, an EMPTY shape, a value that is not WKT
 * and every row past the limit is counted, so the map can say what it left out.
 */
export function collectFeatures(rows: Record<string, unknown>[], column: string, limit: number): FeatureSet {
    const out: FeatureSet = { features: [], nulls: 0, empty: 0, unreadable: 0, capped: 0 };
    for (let row = 0; row < rows.length; row++) {
        if (out.features.length >= limit) {
            out.capped = rows.length - row;
            break;
        }
        const value = rows[row]?.[column];
        if (value === null || value === undefined) {
            out.nulls++;
            continue;
        }
        const geometry = parseWkt(value);
        if (!geometry) out.unreadable++;
        else if (!hasPositions(geometry)) out.empty++;
        else out.features.push({ row, geometry });
    }
    return out;
}

/** The smallest box holding every coordinate drawn; null when nothing is drawn. */
export function boundsOf(features: MapFeature[]): Bounds | null {
    let b: Bounds | null = null;
    for (const f of features) {
        eachPosition(f.geometry, ([x, y]) => {
            if (!b) b = { minX: x, minY: y, maxX: x, maxY: y };
            else {
                if (x < b.minX) b.minX = x;
                if (y < b.minY) b.minY = y;
                if (x > b.maxX) b.maxX = x;
                if (y > b.maxY) b.maxY = y;
            }
        });
    }
    return b;
}

/** CRSs whose coordinates are longitude/latitude degrees. */
const GEOGRAPHIC = new Set([
    'EPSG:4326',
    'OGC:CRS84',
    'CRS84',
    'EPSG:4269',
    'EPSG:4258',
    'EPSG:4283',
    'EPSG:4617',
    'EPSG:4674',
    'EPSG:4979',
]);

/**
 * Whether x and y are longitude and latitude. A named CRS decides. With none,
 * numbers that all fit ±180/±90 are read as degrees, which is what most
 * untagged geometry is (GeoJSON, ST_Point over lon/lat columns).
 */
export function isGeographic(crs: string | null, bounds: Bounds): boolean {
    if (crs) return GEOGRAPHIC.has(crs.trim().toUpperCase());
    return bounds.minX >= -180 && bounds.maxX <= 180 && bounds.minY >= -90 && bounds.maxY <= 90;
}

/** The view that fits `bounds` in a `width` x `height` box with `pad` pixels spare on every side. */
export function fitView(bounds: Bounds, width: number, height: number, pad: number, geographic: boolean): View {
    const midLat = (bounds.minY + bounds.maxY) / 2;
    // Near a pole cos(latitude) heads to zero; a floor keeps the view finite.
    const xStretch = geographic ? Math.max(Math.cos((midLat * Math.PI) / 180), 0.05) : 1;
    const w = (bounds.maxX - bounds.minX) * xStretch;
    const h = bounds.maxY - bounds.minY;
    const roomW = Math.max(width - 2 * pad, 1);
    const roomH = Math.max(height - 2 * pad, 1);
    let scale = Math.min(w > 0 ? roomW / w : Infinity, h > 0 ? roomH / h : Infinity);
    // A single point has no extent to fit; any scale shows it, so pick one.
    if (!Number.isFinite(scale)) scale = Math.min(roomW, roomH);
    const cx = (bounds.minX + bounds.maxX) / 2;
    const cy = (bounds.minY + bounds.maxY) / 2;
    return { scale, xStretch, tx: width / 2 - cx * scale * xStretch, ty: height / 2 + cy * scale };
}

export function toScreen(view: View, [x, y]: Position): Position {
    return [x * view.scale * view.xStretch + view.tx, -y * view.scale + view.ty];
}

export function toMap(view: View, [sx, sy]: Position): Position {
    return [(sx - view.tx) / (view.scale * view.xStretch), (view.ty - sy) / view.scale];
}

/** Zoom by `factor`, keeping the map point under the screen point (sx, sy) where it is. */
export function zoomAt(view: View, factor: number, sx: number, sy: number): View {
    const [mx, my] = toMap(view, [sx, sy]);
    const scale = view.scale * factor;
    return { scale, xStretch: view.xStretch, tx: sx - mx * scale * view.xStretch, ty: sy + my * scale };
}

/**
 * SVG path data for one shape, in map units relative to (ox, oy). The view is
 * applied as a transform; drawing relative to an origin near the data keeps
 * metre-sized coordinates (millions) out of the browser's single-precision SVG
 * arithmetic, which would otherwise shake the shapes at a deep zoom.
 * A point is a zero-length segment: with a round cap and a stroke that does not
 * scale, it draws as a dot of the same size at every zoom.
 */
export function pathParts(g: Geometry, ox: number, oy: number): PathParts {
    const points: string[] = [];
    const lines: string[] = [];
    const polygons: string[] = [];
    const at = ([x, y]: Position) => `${x - ox} ${y - oy}`;
    const line = (ps: Position[]) => ps.map((p, i) => (i === 0 ? 'M' : 'L') + at(p)).join('');
    const ring = (ps: Position[]) => (ps.length > 0 ? line(ps) + 'Z' : '');
    const walk = (s: Geometry): void => {
        switch (s.type) {
            case 'Point':
                points.push(`M${at(s.coordinates)}l0 0`);
                break;
            case 'MultiPoint':
                s.coordinates.forEach(p => points.push(`M${at(p)}l0 0`));
                break;
            case 'LineString':
                lines.push(line(s.coordinates));
                break;
            case 'MultiLineString':
                s.coordinates.forEach(ps => lines.push(line(ps)));
                break;
            case 'Polygon':
                polygons.push(s.coordinates.map(ring).join(''));
                break;
            case 'MultiPolygon':
                s.coordinates.forEach(poly => polygons.push(poly.map(ring).join('')));
                break;
            case 'GeometryCollection':
                s.geometries.forEach(walk);
                break;
        }
    };
    walk(g);
    return { points: points.join(''), lines: lines.join(''), polygons: polygons.join('') };
}
