// Export the Duckle component catalog (ids, labels, kinds, summaries, property
// schemas and ports) to a single JSON file the duckle-mcp crate embeds via
// include_str!. The frontend manifest stays the single source of truth; this
// just serializes it. Run via scripts/build-catalog.mjs (esbuild bundles this
// and stubs the Tauri bridge so it runs under plain Node).
//
// Output path comes from the CATALOG_OUT env var set by the runner.

import { writeFileSync, mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { PALETTE } from '../src/workflow-ui/palette-data';
import { portsForComponent } from '../src/workflow-ui/fields/manifest-synth';
import { getManifest } from '../src/workflow-ui/fields/component-manifests';

// getManifest, NOT synthesizeManifest. getManifest is `MANIFESTS[id] ??
// synthesizeManifest(id)`, and it is what the canvas, the properties panel,
// validation and schema resolution all read. Calling the synthesizer directly
// skipped the 22 hand-authored manifests and exported the generic fallback for
// them instead, so the catalog advertised field keys the engine does not read:
// xf.filter came out as `notes` when the engine reads `predicate`, and xf.sort
// as `column` when the engine reads `orderBy` / `sortColumn`. Since this
// catalog is what duckle-mcp serves to AI agents via list_components and
// get_component_schema, those agents were being handed property names that
// silently produce a no-op node.
// Each component carries the palette category and group it sits in, so a reader
// of the catalog (the website's component reference, an agent) can group the
// components the way the editor does. The order is the palette's, as before.
const components = PALETTE.flatMap((category) =>
    category.groups.flatMap((group) =>
        group.components.map((c) => ({
            id: c.id,
            label: c.label,
            kind: c.kind,
            availability: c.availability,
            summary: c.summary ?? '',
            category: category.label,
            group: group.label,
            ports: portsForComponent(c),
            manifest: getManifest(c.id) ?? null,
        })),
    ),
);

const catalog = {
    version: '1',
    componentCount: components.length,
    components,
};

const out = process.env.CATALOG_OUT;
if (!out) {
    throw new Error('CATALOG_OUT env var not set');
}
mkdirSync(dirname(out), { recursive: true });
writeFileSync(out, JSON.stringify(catalog, null, 2) + '\n');
// eslint-disable-next-line no-console
console.error(`export-catalog: wrote ${components.length} components to ${out}`);
