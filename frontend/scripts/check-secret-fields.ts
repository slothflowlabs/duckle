// A value the engine calls a credential must not sit on screen in the clear.
//
// The engine's `is_secret_prop_key` (crates/duckdb-engine/src/util.rs) decides
// what gets redacted from run logs and refused on deploy. The form decides what
// is masked. Those two answers drifted: `password` was declared by 46 fields
// and marked secret on one, so 45 connectors showed a password as plain text
// while the engine was carefully keeping it out of logs.
//
// `withMaskedCredentials` in component-manifests.ts now masks them centrally.
// This check exists for the case that pass cannot cover: a NEW credential
// property the engine starts redacting that nobody adds to the form's list.
//
// The needles are READ FROM THE RUST SOURCE rather than copied here, because a
// copied list is the drift this is meant to catch.
//
// Run via scripts/check-secret-fields.mjs.

import { readFileSync } from 'node:fs';
import { getManifest } from '../src/workflow-ui/fields/component-manifests';
import { PALETTE } from '../src/workflow-ui/palette-data';
import {
    NAMES_SOMETHING_PUBLIC as TS_PUBLIC,
    SECRET_HEADERS as TS_HEADERS,
    SECRET_NEEDLES as TS_NEEDLES,
} from '../src/secret-keys';

// The runner passes this. `import.meta.url` cannot be used: esbuild bundles
// this file into a temp directory, so it would resolve relative to there - the
// first run failed looking for crates/ under AppData.
const utilRs = process.env.DUCKLE_UTIL_RS;
if (!utilRs) throw new Error('DUCKLE_UTIL_RS is not set; run via scripts/check-secret-fields.mjs');
const rust = readFileSync(utilRs, 'utf8');

function arrayAfter(marker: string): string[] {
    const at = rust.indexOf(marker);
    if (at < 0) throw new Error(`${marker} not found in util.rs - has the engine moved it?`);
    // After the `= `, not the first `[`: the first one belongs to the type
    // annotation, `[&str; 8]`, which holds no strings. Parsed that way the
    // exclusion list came back empty and the check reported tokenUrl and
    // saslUsername as exposed - keys the engine had just stopped calling
    // credentials.
    const eq = rust.indexOf('= [', at);
    if (eq < 0) throw new Error(`${marker} has no value array`);
    const close = rust.indexOf('];', eq);
    const found = [...rust.slice(eq, close).matchAll(/"([^"]+)"/g)].map(m => m[1]);
    if (!found.length) throw new Error(`${marker} parsed as empty`);
    return found;
}

// The needle list is_secret_prop_key matches on, and the whole-key exclusions.
const fnAt = rust.indexOf('pub fn is_secret_prop_key');
if (fnAt < 0) throw new Error('is_secret_prop_key not found in util.rs');
const NOT_SECRET = arrayAfter('const NAMES_SOMETHING_PUBLIC');
const NEEDLES = arrayAfter('pub const SECRET_NEEDLES');
if (NEEDLES.length < 10) throw new Error(`only parsed ${NEEDLES.length} needles from util.rs`);
const HEADERS = arrayAfter('pub const SECRET_HEADERS');

// Generated pipeline documentation withholds by the same rule, from a copy of these
// lists in src/secret-keys.ts. A copy that drifted would put a credential into a page
// meant to be shared, so the two must match exactly.
{
    const sorted = (xs: readonly string[]) => [...xs].sort().join(', ');
    const drift: string[] = [];
    if (sorted(TS_NEEDLES) !== sorted(NEEDLES)) drift.push(`SECRET_NEEDLES\n    util.rs:        ${sorted(NEEDLES)}\n    secret-keys.ts: ${sorted(TS_NEEDLES)}`);
    if (sorted(TS_PUBLIC) !== sorted(NOT_SECRET)) drift.push(`NAMES_SOMETHING_PUBLIC\n    util.rs:        ${sorted(NOT_SECRET)}\n    secret-keys.ts: ${sorted(TS_PUBLIC)}`);
    if (sorted(TS_HEADERS) !== sorted(HEADERS)) drift.push(`SECRET_HEADERS\n    util.rs:        ${sorted(HEADERS)}\n    secret-keys.ts: ${sorted(TS_HEADERS)}`);
    if (drift.length) {
        console.error(`\nsrc/secret-keys.ts no longer matches is_secret_prop_key in util.rs:\n  ${drift.join('\n  ')}`);
        process.exit(1);
    }
}

const enginesSecret = (key: string): boolean => {
    const k = key.toLowerCase();
    if (k === 'pat') return true;
    if (NOT_SECRET.includes(k)) return false;
    return NEEDLES.some(n => k.includes(n));
};

// Visible on purpose. Each names something the reader has to be able to read.
const ALLOWED_VISIBLE: Record<string, string> = {
    accessKeyId: 'an identifier, not a secret - AWS publishes it in ARNs; secretAccessKey is the secret',
};

const components = [];
for (const category of PALETTE) for (const group of category.groups) components.push(...group.components);

const masked = (f: { secret?: boolean; placeholder?: string }) =>
    f.secret === true || f.placeholder === '•'.repeat(8);

const exposed = new Map<string, string[]>();
let scanned = 0;
for (const component of components) {
    let m;
    try {
        m = getManifest(component.id);
    } catch {
        continue;
    }
    if (!m?.sections) continue;
    for (const section of m.sections) {
        for (const field of section.fields) {
            // Only `text` has a masked renderer (TextField). An expression or
            // file-path field is a different problem and is not pretended away
            // by setting a flag its renderer ignores.
            if (field.kind !== 'text') continue;
            if (!enginesSecret(field.key)) continue;
            scanned++;
            if (masked(field) || ALLOWED_VISIBLE[field.key]) continue;
            exposed.set(field.key, [...(exposed.get(field.key) ?? []), component.id]);
        }
    }
}

console.log(
    `check-secret-fields: ${scanned} credential text fields across ${components.length} components ` +
        `(${NEEDLES.length} needles read from util.rs)`,
);
for (const [k, why] of Object.entries(ALLOWED_VISIBLE)) console.log(`  visible on purpose: ${k} - ${why}`);

if (exposed.size) {
    console.error(`\n${exposed.size} credential field(s) shown in the clear:\n`);
    for (const [key, comps] of exposed) {
        console.error(`  ${key}: ${comps.length} component(s) - ${comps.slice(0, 5).join(', ')}`);
    }
    console.error(
        '\nAdd the key to CREDENTIAL_KEYS in component-manifests.ts, or to ' +
            'ALLOWED_VISIBLE here with the reason it must stay readable.',
    );
    process.exit(1);
}
console.log('every credential text field is masked');
