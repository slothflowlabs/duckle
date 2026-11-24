// What a stage looks like, shared by the swarm field and the panel's tabs.
// Colours are theme tokens only, so both themes stay on brand.

import { DraftingCompass, FlaskConical, Hammer, ScanSearch, ShieldCheck, Wrench } from 'lucide-react';
import type { StageId } from './swarm-state';

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
