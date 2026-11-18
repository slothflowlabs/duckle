// The frontend's dependency-advisory gate: `npm audit --audit-level=high`, with
// named advisories skipped while they have no fix. npm audit keeps no ignore list
// of its own, so this judges its JSON report.
//
// Each skip points at the section of docs/security/reference/dependency-advisories.md
// that says why it is accepted. A skip ends by itself: once `npm audit fix` can fix
// the advisory without a major version, the gate fails again, and running it is
// the next step.
//
// Run from frontend/: node scripts/audit-gate.mjs
import { execSync } from 'node:child_process';

export const SKIPPED_ADVISORIES = {
    // picomatch <= 4.0.7, ReDoS through repeated extglobs. No patched release.
    // vite matches files with it at build time; it is not in the shipped bundle.
    'GHSA-fc5q-39xc-vjpf': 'docs/security/reference/dependency-advisories.md#ghsa-fc5q-39xc-vjpf---picomatch-407-redos-through-repeated-extglobs',
};

const FAILING = new Set(['high', 'critical']);

/** Whether `npm audit fix` can fix it without --force: true, or an update that is not a major version. */
const fixable = fix => fix === true || (typeof fix === 'object' && fix !== null && fix.isSemVerMajor === false);

/**
 * Judge an `npm audit --json` report. Every high or critical advisory fails, except
 * a skipped one while it cannot be fixed. No report fails as well: a gate that
 * passes when the audit did not run would prove nothing.
 */
export function judge(report, skipped = SKIPPED_ADVISORIES) {
    if (report?.auditReportVersion !== 2 || typeof report.vulnerabilities !== 'object' || report.vulnerabilities === null) {
        return { failures: [`npm audit gave no report: ${report?.error?.summary ?? 'not an audit report (version 2)'}`], skipping: [] };
    }
    const failures = [];
    const skipping = [];
    for (const [name, vuln] of Object.entries(report.vulnerabilities)) {
        for (const via of vuln.via ?? []) {
            // A name is a dependency flagged for one of its own; that advisory is judged there.
            if (typeof via === 'string' || !FAILING.has(via.severity)) continue;
            const id = String(via.url ?? '').split('/').pop();
            const line = `${name}: ${via.title} (${id}, ${via.severity})`;
            if (!Object.hasOwn(skipped, id)) {
                failures.push(line);
            } else if (fixable(vuln.fixAvailable)) {
                failures.push(`${line}: a fix is out, so it is no longer skipped; run npm audit fix, then drop the skip`);
            } else {
                skipping.push(`${line}, see ${skipped[id]}`);
            }
        }
    }
    return { failures, skipping };
}

function main() {
    let out = '';
    try {
        out = execSync('npm audit --json', { encoding: 'utf8', stdio: ['ignore', 'pipe', 'inherit'], maxBuffer: 64 * 1024 * 1024 });
    } catch (e) {
        // npm audit exits 1 when it reports anything; the report is still on stdout.
        out = e.stdout ?? '';
    }
    let report = null;
    try {
        report = JSON.parse(out);
    } catch {
        // Judged as no report.
    }
    const { failures, skipping } = judge(report);
    for (const line of skipping) console.log(`skipped: ${line}`);
    if (failures.length > 0) {
        console.error(`\nnpm audit: ${failures.length} advisory(ies) fail the gate:\n`);
        for (const line of failures) console.error(`  - ${line}`);
        process.exit(1);
    }
    console.log('npm audit: no high or critical advisory, other than those skipped');
}

// Only as the script itself: check-logic imports judge() into a bundle of its own.
if (process.argv[1]?.endsWith('audit-gate.mjs')) main();
