export type ArrangeMode = 'left' | 'right' | 'top' | 'bottom' | 'distribute-x' | 'distribute-y';

type Box = { id: string; position: { x: number; y: number }; measured?: { width?: number; height?: number } };

/**
 * Where these nodes go to line them up: only the ones that move.
 *
 * Aligning uses the selection's outermost edge. Distributing keeps the first and
 * last in place and spaces the rest so every gap between neighbours is equal,
 * which is what Hop and Talend do; it needs three nodes to mean anything.
 */
export function arrange(nodes: Box[], mode: ArrangeMode): { id: string; position: { x: number; y: number } }[] {
    const w = (n: Box) => n.measured?.width ?? 0;
    const h = (n: Box) => n.measured?.height ?? 0;
    const to = (n: Box, x: number, y: number) => ({ id: n.id, position: { x, y } });
    let placed: { id: string; position: { x: number; y: number } }[];
    switch (mode) {
        case 'left': {
            const edge = Math.min(...nodes.map(n => n.position.x));
            placed = nodes.map(n => to(n, edge, n.position.y));
            break;
        }
        case 'right': {
            const edge = Math.max(...nodes.map(n => n.position.x + w(n)));
            placed = nodes.map(n => to(n, edge - w(n), n.position.y));
            break;
        }
        case 'top': {
            const edge = Math.min(...nodes.map(n => n.position.y));
            placed = nodes.map(n => to(n, n.position.x, edge));
            break;
        }
        case 'bottom': {
            const edge = Math.max(...nodes.map(n => n.position.y + h(n)));
            placed = nodes.map(n => to(n, n.position.x, edge - h(n)));
            break;
        }
        case 'distribute-x':
        case 'distribute-y': {
            if (nodes.length < 3) return [];
            const horizontal = mode === 'distribute-x';
            const start = (n: Box) => (horizontal ? n.position.x : n.position.y);
            const size = horizontal ? w : h;
            const order = [...nodes].sort((a, b) => start(a) - start(b));
            const first = order[0];
            const last = order[order.length - 1];
            const span = start(last) + size(last) - start(first);
            const gap = (span - order.reduce((sum, n) => sum + size(n), 0)) / (order.length - 1);
            let at = start(first);
            placed = order.map((n, i) => {
                // The ends stay exactly where they are, whatever the rounding.
                const here = i === order.length - 1 ? start(last) : at;
                const p = horizontal ? to(n, here, n.position.y) : to(n, n.position.x, here);
                at += size(n) + gap;
                return p;
            });
            break;
        }
    }
    const was = new Map(nodes.map(n => [n.id, n.position]));
    return placed.filter(p => {
        const old = was.get(p.id)!;
        return old.x !== p.position.x || old.y !== p.position.y;
    });
}
