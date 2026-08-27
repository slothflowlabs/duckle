// Bundle + run scripts/export-catalog.ts under plain Node to produce
// crates/duckle-mcp/catalog.json.
//
// manifest-synth.ts imports the Tauri bridge (@tauri-apps/api), which has no
// meaning outside a Tauri window. The component property schemas it builds do
// not actually call those APIs, so we bundle with esbuild and stub the bridge
// + @tauri-apps modules to no-ops, then import the bundle to run it.
//
// esbuild is a direct devDependency. It used to arrive transitively through
// vite, which stopped being true at vite 8 - that bundles with rolldown, so
// the transitive esbuild vanished and this script broke.

import esbuild from 'esbuild';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import { rmSync } from 'node:fs';

const here = dirname(fileURLToPath(import.meta.url));
const entry = resolve(here, 'export-catalog.ts');
const out = resolve(here, '../../crates/duckle-mcp/catalog.json');
const bundle = resolve(tmpdir(), `duckle-export-catalog-${process.pid}.mjs`);

const stubTauri = {
    name: 'stub-tauri',
    setup(build) {
        // Anything that needs a live Tauri/browser context becomes a no-op.
        build.onResolve({ filter: /(^|\/)tauri-bridge$/ }, () => ({ path: 'stub', namespace: 'stub' }));
        build.onResolve({ filter: /(^|\/)tauri-dialog$/ }, () => ({ path: 'stub', namespace: 'stub' }));
        build.onResolve({ filter: /^@tauri-apps\// }, () => ({ path: 'stub', namespace: 'stub' }));
        build.onLoad({ filter: /.*/, namespace: 'stub' }, () => ({
            contents:
                'export const tauriAutodetect = async () => null;' +
                'export const invoke = async () => null;' +
                'export class Channel {}' +
                'export const isTauri = () => false;' +
                'export default {};',
            loader: 'js',
        }));
    },
};

await esbuild.build({
    entryPoints: [entry],
    bundle: true,
    platform: 'node',
    format: 'esm',
    outfile: bundle,
    plugins: [stubTauri],
    logLevel: 'warning',
});

process.env.CATALOG_OUT = out;
await import('file://' + bundle.replace(/\\/g, '/'));
rmSync(bundle, { force: true });
