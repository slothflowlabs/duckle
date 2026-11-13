// Behaviour checks for frontend logic that nothing else tests.
//
// The frontend has no unit-test runner. A bug hunt on 2026-09-17 found logic
// errors that reading the code did not show and only EXECUTING it did, so each
// check here calls the real module with a real input and fails the build if the
// answer is wrong. Run via scripts/check-logic.mjs, which bundles this file.

import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import type { Edge, Node } from '@xyflow/react';
import type { DuckleNodeData } from '../src/pipeline-types';
import type { RepoItem } from '../src/repo-types';
import { livePreviewable } from '../src/live-preview';
import { buildContextVars, discoverParams, pipelineRunFields, resolveForRun, resolveTimeBuiltin } from '../src/run-resolve';
import { conditionToSql, type FilterOp } from '../src/workflow-ui/fields/FilterBuilderField';
import { scheduleActionError, scheduleForSave, serverSchedule } from '../src/schedule-save';
import { pickNamesNodeConnection } from '../src/workflow-ui/fields/ConnectionRefField';
import { UndoHistory, type CanvasSnapshot } from '../src/undo-history';
import { saveItemPayload, saveNow } from '../src/workspace';
import { annotationPatch } from '../src/catalog-annotate';
import { CONNECTION_TYPES } from '../src/workflow-ui/editors/ConnectionEditorModal';
import { gitActionRewritesFiles } from '../src/git-actions';
import { validatePipeline } from '../src/validation';
import { deriveNodeSubtitle } from '../src/node-subtitle';
import { resolveOutputSchema } from '../src/schema-resolve';
import { placeTip } from '../src/tour-place';
import { pipelinesUsingConnections } from '../src/connection-usage';
import { arrange } from '../src/canvas/arrange';
import { pipelineDocument } from '../src/pipeline-doc';
import { parseWkt } from '../src/geo/wkt';
import { linkPort, linkRows, withBreakpoint } from '../src/link-rows';
import {
    edgesForDisplay,
    frameRect,
    groupNodes,
    hiddenSteps,
    memberMoves,
    pruneGroups,
    ungroupNode,
    type PipelineGroup,
} from '../src/canvas/groups';
import {
    boundsOf,
    collectFeatures,
    crsOfType,
    fitView,
    geometryColumns,
    isGeographic,
    pathParts,
    toMap,
    toScreen,
    zoomAt,
} from '../src/geo/map-view';
import { getManifest } from '../src/workflow-ui/fields/component-manifests';
import { batchesOf, describeBatch, restoredRepo, trashEntry, trashFileName } from '../src/recycle-bin';
import { subflowPorts, subflowStepLabels } from '../src/canvas/subflow-ports';
import { extractSubflow } from '../src/canvas/subflow-extract';
import { caseProblem, cleaned, dummyInput, guessGenerator, listOf, newCase, newSuite } from '../src/test-cases';
import {
    buildBundle,
    cancelPipeline,
    engineInstall,
    mcpConnectionInfo,
    runPipeline,
    runPipelinePartial,
    scheduleDelete,
    scheduleList,
    scheduleRunNow,
    scheduleUpsert,
    settingsSetAi,
    settingsSetAllowUnsigned,
    settingsSetContextFile,
    settingsSetMemoryLimit,
    settingsSetPower,
    runHistory,
    settingsSetProxy,
    watermarkClear,
    watermarkList,
    watermarkSet,
    type RunResult,
    type Schedule,
} from '../src/tauri-bridge';

// The frontend directory, injected by check-logic.mjs: the bundle runs from a
// temp dir, so neither import.meta.url nor the cwd can be trusted to find it.
declare const __FRONTEND_DIR__: string;

const failures: string[] = [];

function check(name: string, ok: boolean, detail: string): void {
    if (!ok) failures.push(`${name}\n      ${detail}`);
}

function node(id: string, componentId: string, properties: Record<string, unknown>): Node<DuckleNodeData> {
    return {
        id,
        position: { x: 0, y: 0 },
        data: { label: id, componentId, properties },
    } as unknown as Node<DuckleNodeData>;
}

function context(name: string, vars: Record<string, string>): RepoItem {
    return {
        id: name,
        name,
        type: 'context',
        payload: { variables: Object.entries(vars).map(([key, value]) => ({ key, value })) },
    } as unknown as RepoItem;
}

// ---------------------------------------------------------------------------
// A name a ctl.setvar node sets is filled in by the RUN, not by a static context.
//
// The README promises it and the engine enforces it on the headless path
// (context.rs skips plan::run_var_names). The desktop resolves placeholders here
// before the engine ever sees them, and did not skip them: a context entry of the
// same name replaced ${batch_date} with its static default, so the run variable
// was never read. With an empty default the run failed on a conversion error;
// with a real one it succeeded and wrote the WRONG rows.
// ---------------------------------------------------------------------------
{
    const nodes = [
        node('s', 'ctl.setvar', { name: 'batch_date', value: 'max(d)' }),
        node('q', 'code.sql', {
            sql: "SELECT * FROM input WHERE d = '${batch_date}' AND r = '${REGION}'",
        }),
    ];
    for (const staticDefault of ['', '2020-01-01']) {
        const out = resolveForRun(nodes, [context('dev', { batch_date: staticDefault, REGION: 'EU' })]);
        const sql = String(out[1].data.properties?.sql);
        check(
            `setvar: a static context default of ${JSON.stringify(staticDefault)} does not pre-empt the run variable`,
            sql.includes('${batch_date}'),
            `the placeholder was replaced before the run could fill it: ${sql}`,
        );
        check(
            `setvar: an ordinary context variable still resolves beside it (default ${JSON.stringify(staticDefault)})`,
            sql.includes("r = 'EU'"),
            `excluding the run variable broke an unrelated one: ${sql}`,
        );
    }

    const params = discoverParams(nodes, {});
    check(
        'setvar: the editor does not prompt for a name a node sets',
        !params.includes('batch_date'),
        `prompted for it, and a typed value would override the run's own: ${JSON.stringify(params)}`,
    );
    check(
        'setvar: an unset ordinary placeholder is still prompted for',
        params.includes('REGION'),
        `stopped prompting for REGION: ${JSON.stringify(params)}`,
    );

    const blank = [node('s', 'ctl.setvar', { name: '   ' }), node('q', 'code.sql', { sql: "SELECT '${X}'" })];
    const resolved = String(resolveForRun(blank, [context('dev', { X: 'x' })])[1].data.properties?.sql);
    check(
        'setvar: a node with a blank name excludes nothing',
        resolved === "SELECT 'x'",
        `a blank name must not stop ordinary resolution: ${resolved}`,
    );
}

// ---------------------------------------------------------------------------
// Live mode never runs up to a sink.
//
// A preview is a partial run to the target, so previewing a sink writes. The
// select and toggle triggers skipped sinks; the edit trigger did not, and typing
// into a sink's path wrote to the half-typed path.
// ---------------------------------------------------------------------------
{
    for (const sink of ['snk.parquet', 'snk.csv', 'snk.postgres', 'snk.rest']) {
        check(`live preview: ${sink} is never previewed`, !livePreviewable(sink), 'a preview would write');
    }
    for (const other of ['src.csv', 'xf.filter', 'code.sql', 'qa.expect']) {
        check(`live preview: ${other} is still previewed`, livePreviewable(other), 'the guard is too broad');
    }
    check(
        'live preview: a node with no component id is not refused as a sink',
        livePreviewable(undefined) && livePreviewable(''),
        'an unknown node must not be mistaken for a sink',
    );

    // The rule only helps if the place every trigger reaches applies it. App.tsx
    // has no harness to render, so this reads the preview entry point: it must
    // ask before it claims the run slot, or a trigger that skips its own check
    // (the edit one did) still writes.
    const app = readFileSync(resolve(__FRONTEND_DIR__, 'src/App.tsx'), 'utf8');
    const entry = app.indexOf('const triggerLivePreview = useCallback(');
    const claim = entry < 0 ? -1 : app.indexOf('isRunningRef.current = true;', entry);
    check(
        'live preview: the preview entry point is still where it was',
        entry >= 0 && claim > entry,
        'triggerLivePreview moved or changed shape; update this check to follow it',
    );
    check(
        'live preview: the preview entry point refuses a sink before starting a run',
        claim > entry && app.slice(entry, claim).includes('livePreviewable('),
        'triggerLivePreview starts a partial run without asking livePreviewable, so editing a sink writes',
    );
}

// ---------------------------------------------------------------------------
// The filter builder's "contains", "starts with" and "ends with" are literal.
//
// They compiled to LIKE with the typed value spliced into the pattern, so a %
// or _ in it was a wildcard: "contains 50%" also kept "50 units", and "starts
// with a_b" kept "axb". The generated SQL runs in DuckDB (plan/builders.rs
// build_filter). The oracle below is SQL LIKE itself - % any run, _ one
// character, the ESCAPE character makes the next one literal, anything else
// literal, whole string - checked once against DuckDB 1.5.4, so each generated
// pattern must keep exactly the rows plain string matching keeps.
// ---------------------------------------------------------------------------
{
    const likeMatches = (sql: string, text: string): boolean | string => {
        const m = /^"c" (?:NOT )?LIKE '((?:[^']|'')*)'(?: ESCAPE '((?:[^']|'')*)')?$/.exec(sql);
        if (!m) return `not a LIKE this oracle understands: ${sql}`;
        const pattern = m[1].replace(/''/g, "'");
        const escape = m[2]?.replace(/''/g, "'");
        let re = '';
        for (let i = 0; i < pattern.length; i++) {
            const ch = pattern[i];
            if (escape !== undefined && ch === escape && i + 1 < pattern.length) {
                re += pattern[++i].replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
            } else if (ch === '%') re += '[\\s\\S]*';
            else if (ch === '_') re += '[\\s\\S]';
            else re += ch.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        }
        return new RegExp('^' + re + '$').test(text);
    };
    const literal: Record<string, (text: string, v: string) => boolean> = {
        contains: (t, v) => t.includes(v),
        starts_with: (t, v) => t.startsWith(v),
        ends_with: (t, v) => t.endsWith(v),
    };
    const values = ['50%', 'a_b', 'back\\slash', "it's", '100%_\\', 'plain'];
    const texts = [
        '50%', '50 units', '150%', 'a_b', 'axb', 'a_bc', 'xa_b', 'back\\slash', 'backslash', "it's",
        'its', '100%_\\', '100xy\\', '100%_\\ tail', 'plain', 'plainer', 'a plain', '',
    ];
    for (const op of Object.keys(literal) as FilterOp[]) {
        for (const v of values) {
            const sql = conditionToSql({ id: 'x', column: 'c', op, value: v }, 'string');
            for (const t of texts) {
                const got = likeMatches(sql, t);
                const want = literal[op](t, v);
                check(
                    `filter builder: ${op} ${JSON.stringify(v)} on ${JSON.stringify(t)}`,
                    got === want,
                    typeof got === 'string' ? got : `kept=${got}, a literal ${op} keeps=${want}; SQL: ${sql}`,
                );
            }
        }
    }
    // "matches" is the one op whose value IS a pattern; it must stay one.
    check(
        'filter builder: "matches" still treats % as a wildcard',
        likeMatches(conditionToSql({ id: 'x', column: 'c', op: 'like', value: '50%' }, 'string'), '50 units') === true,
        'the escaping leaked into the pattern operator',
    );
}

// ---------------------------------------------------------------------------
// Saving in the desktop Schedules dialog keeps what the dialog does not show.
//
// A schedule's timezone, exclusion calendar, misfire policy and catch-up bounds
// are set through the server API. The dialog has no control for them and built
// a fresh record from its own fields, and the desktop upsert replaces the whole
// record, so renaming a schedule put a Brussels 03:00 job back on the machine's
// clock and switched its maintenance calendar off.
// ---------------------------------------------------------------------------
{
    const loaded: Schedule = {
        id: 's1',
        pipeline_id: 'p1',
        name: 'Nightly',
        enabled: true,
        kind: { type: 'cron', expr: '0 0 3 * * *' },
        timezone: 'Europe/Brussels',
        exclude: { weekdays: ['sunday'], dates: ['2026-12-25'] },
        misfire: 'latest',
        catchup: { maxCatchupRuns: 5, maxCatchupAgeDays: 7 },
    };
    const saved = scheduleForSave(loaded, {
        id: 's1',
        pipelineId: 'p1',
        name: '  Nightly load  ',
        enabled: false,
        kind: { type: 'cron', expr: '0 30 3 * * *' },
    });
    const kept = (k: keyof Schedule) => JSON.stringify(saved[k]) === JSON.stringify(loaded[k]);
    for (const k of ['timezone', 'exclude', 'misfire', 'catchup'] as const) {
        check(
            `schedule save: ${k} survives an edit in the dialog`,
            kept(k),
            `sent ${JSON.stringify(saved[k])}, the store had ${JSON.stringify(loaded[k])}`,
        );
    }
    check(
        'schedule save: the fields the dialog edits are the edited values',
        saved.name === 'Nightly load' &&
            saved.enabled === false &&
            saved.kind.type === 'cron' &&
            saved.kind.expr === '0 30 3 * * *',
        `got ${JSON.stringify(saved)}`,
    );
    const fresh = scheduleForSave(undefined, {
        id: '',
        pipelineId: 'p1',
        name: ' ',
        enabled: true,
        kind: { type: 'interval', seconds: 60 },
    });
    check(
        'schedule save: a new schedule carries no settings it was never given',
        fresh.timezone === undefined && fresh.exclude === undefined && fresh.name === 'Schedule',
        `got ${JSON.stringify(fresh)}`,
    );
}

// ---------------------------------------------------------------------------
// Picking a REST node's HTTP transport does not replace its saved connection.
//
// Every connection-ref field called onPickConnection, which writes
// connectionRef. For transportRef that swapped the node's auth connection for
// the transport, and the two updates both spread the pre-pick properties, so the
// transport pick itself was erased.
// ---------------------------------------------------------------------------
{
    check(
        'connection pick: the saved-connection field names the node connection',
        pickNamesNodeConnection({ key: 'connectionRef' }),
        'picking a saved connection no longer sets connectionRef',
    );
    check(
        'connection pick: the HTTP transport field does not',
        !pickNamesNodeConnection({ key: 'transportRef' }),
        'picking a transport would replace the node connection',
    );
    const field = readFileSync(resolve(__FRONTEND_DIR__, 'src/workflow-ui/fields/ConnectionRefField.tsx'), 'utf8');
    const start = field.indexOf('const handleChange = ');
    const hook = start < 0 ? -1 : field.indexOf('onPickConnection(', start);
    check(
        'connection pick: the change handler is still where it was',
        start >= 0 && hook > start,
        'handleChange moved or changed shape; update this check to follow it',
    );
    check(
        'connection pick: the change handler asks before calling onPickConnection',
        hook > start && field.slice(start, hook).includes('pickNamesNodeConnection(field)'),
        'handleChange calls onPickConnection for every connection-ref field, transportRef included',
    );
}

// ---------------------------------------------------------------------------
// Undo reaches every edit, and only edits.
//
// Three ways a step was lost: an edit the history key could not see (a node's
// SQL name, a declared schema) was not a step, so the next undo reverted it
// along with the step before; an undo pressed while an edit was still inside
// its 350 ms debounce skipped that edit for good; and switching pipeline inside
// the debounce cancelled the pending edit. Driven with a fake clock, so "inside
// the debounce" is exact.
// ---------------------------------------------------------------------------
{
    const timers = new Map<number, () => void>();
    let nextTimer = 0;
    const timer = (fn: () => void, _ms: number) => {
        const id = nextTimer++;
        timers.set(id, fn);
        return () => {
            timers.delete(id);
        };
    };
    const settle = () => {
        for (const [id, fn] of [...timers]) {
            timers.delete(id);
            fn();
        }
    };
    const snap = (data: Record<string, unknown>, x = 0): CanvasSnapshot => ({
        nodes: [{ id: 'n', position: { x, y: 0 }, data: { label: 'n', componentId: 'xf.filter', ...data } }],
        edges: [],
    });
    const fresh = (initial: CanvasSnapshot) => {
        timers.clear();
        const h = new UndoHistory('a', initial, timer, () => {});
        h.observe('a', initial);
        return h;
    };
    const same = (a: CanvasSnapshot | null, b: CanvasSnapshot) => a === b;

    {
        const s0 = snap({});
        const h = fresh(s0);
        const s1 = snap({ alias: 'orders' });
        h.observe('a', s1);
        settle();
        check('undo: renaming a node\'s SQL name is a step', same(h.undo(), s0), 'undo did not return to the old name');
    }
    {
        const s0 = snap({ schema: [{ name: 'id', type: 'int64' }] });
        const h = fresh(s0);
        const s1 = snap({ schema: [{ name: 'id', type: 'string' }] });
        h.noteEdit();
        h.observe('a', s1);
        settle();
        check('undo: editing a declared schema is a step', same(h.undo(), s0), 'undo did not return to the old schema');
    }
    {
        const s0 = snap({});
        const h = fresh(s0);
        h.observe('a', snap({ schema: [{ name: 'id', type: 'int64' }], sampleRows: [{ id: 1 }] }));
        settle();
        check('undo: a run preview is not a step', h.undo() === null, 'a run filling schema and rows became undoable');
    }
    {
        const s0 = snap({}, 0);
        const h = fresh(s0);
        const s1 = snap({}, 100);
        h.observe('a', s1);
        settle();
        const s2 = snap({}, 200);
        h.observe('a', s2); // still inside the debounce
        const u = h.undo();
        check('undo: an undo inside the debounce goes back one step, not two', same(u, s1), `went to x=${u?.nodes[0].position.x}`);
        if (u) h.observe('a', u);
        const r = h.redo();
        check('undo: and redo brings the edit back', same(r, s2), `redo went to x=${r?.nodes[0].position.x}`);
    }
    {
        const s0 = snap({}, 0);
        const h = fresh(s0);
        h.observe('a', snap({}, 100));
        h.observe('a', snap({ sampleRows: [{ id: 1 }] }, 100)); // preview lands inside the debounce
        settle();
        check('undo: a preview inside the debounce does not cancel the pending edit', same(h.undo(), s0), 'the move was lost');
    }
    {
        const s0 = snap({}, 0);
        const h = fresh(s0);
        const s1 = snap({}, 100);
        h.observe('a', s1);
        h.observe('b', snap({}, 5)); // switch pipeline inside the debounce
        settle();
        h.observe('a', s1);
        check('undo: switching pipeline inside the debounce keeps the edit', same(h.undo(), s0), 'the move was lost');
    }
    {
        const s0 = snap({}, 0);
        const h = fresh(s0);
        h.observe('a', snap({}, 100));
        h.observe('a', snap({}, 0)); // dragged back where it was
        settle();
        check('undo: a burst that ends where it started is not a step', h.undo() === null, 'a no-op burst became a step');
    }
    {
        // History was keyed by pipeline id alone, and ids repeat across
        // workspaces (every new one starts at j1). After switching workspace, Ctrl+Z
        // put the OTHER workspace's pipeline onto this canvas, and autosave wrote it.
        timers.clear();
        const other = snap({}, 0);
        const h = new UndoHistory('a', other, timer, () => {});
        h.observe('a', other, 'C:/work/one');
        h.observe('a', snap({}, 100), 'C:/work/one');
        settle();
        const mine = snap({ label: 'mine' }, 500);
        h.observe('a', mine, 'C:/work/two');
        const u = h.undo();
        check(
            'undo: switching workspace leaves nothing of the previous one to undo into',
            u === null,
            `undo restored a pipeline from the previous workspace (x=${u?.nodes[0].position.x})`,
        );
    }
}

// ---------------------------------------------------------------------------
// A connection whose secrets cannot be encrypted is not written in clear text.
//
// The server refuses to encrypt for a role that may not, and says encrypting is
// strict so a failure never falls through to plaintext. The editor caught the
// refusal and wrote the payload it had been given, password and all. Driven
// through the real saveItemPayload against a fake web backend that records
// every file write.
// ---------------------------------------------------------------------------
{
    type Invoke = (cmd: string, args: Record<string, unknown>) => Promise<unknown>;
    const g = globalThis as unknown as { fetch: typeof fetch; __checkLogicInvoke?: Invoke };
    const realFetch = g.fetch;
    const writes: string[] = [];
    g.fetch = (async (url: string, init?: { body?: string }) => {
        const op = String(url).replace('/api/fs/', '');
        const body = JSON.parse(init?.body ?? '{}') as { content?: string };
        if (op === 'write') writes.push(body.content ?? '');
        return new Response(JSON.stringify(op === 'exists' ? { exists: true } : {}), { status: 200 });
    }) as unknown as typeof fetch;
    const secret = 'hunter2-not-for-disk';
    const payload = { kind: 'postgres', host: 'db.local', username: 'u', password: secret };
    const backends: [string, Invoke][] = [
        ['refused by the server', async () => {
            throw new Error('connection_encrypt_payload: HTTP 403 forbidden');
        }],
        ['missing from the backend (a 404 the shim turns into null)', async () => null],
    ];
    // saveItemPayload logs the refusal it is expected to hit here.
    const realError = console.error;
    console.error = () => {};
    for (const [why, invoke] of backends) {
        writes.length = 0;
        g.__checkLogicInvoke = invoke;
        const ok = await saveItemPayload('/ws', 'connection', 'c1', payload);
        check(
            `connection save: encryption ${why} writes nothing`,
            writes.length === 0,
            `wrote ${JSON.stringify(writes)}`,
        );
        check(`connection save: encryption ${why} reports a failed save`, ok === false, 'reported saved');
    }
    console.error = realError;
    writes.length = 0;
    g.__checkLogicInvoke = async (_cmd, args) =>
        JSON.stringify({ ...JSON.parse(String(args.payloadJson)), password: 'enc:v2:sealed' });
    const ok = await saveItemPayload('/ws', 'connection', 'c1', payload);
    check(
        'connection save: an encrypted payload is still written',
        ok === true && writes.length === 1 && writes[0].includes('enc:v2:sealed') && !writes[0].includes(secret),
        `ok=${ok}, wrote ${JSON.stringify(writes)}`,
    );
    g.__checkLogicInvoke = undefined;
    g.fetch = realFetch;
}

// ---------------------------------------------------------------------------
// A git pull or checkout reloads the workspace instead of saving over it.
//
// Both rewrite files on disk while the editor keeps what it loaded before, and
// the next edit autosaved those old copies over what git had brought in. The
// editor already has a reload for exactly this (#92); nothing called it.
// ---------------------------------------------------------------------------
{
    for (const label of ['pull', 'checkout']) {
        check(`git: ${label} rewrites workspace files`, gitActionRewritesFiles(label), 'the editor would keep stale copies');
    }
    for (const label of ['init', 'commit', 'push', 'remote', 'branch-create', 'save-pat', 'clear-pat']) {
        check(`git: ${label} does not reload the workspace`, !gitActionRewritesFiles(label), 'a needless reload');
    }
    const panel = readFileSync(resolve(__FRONTEND_DIR__, 'src/workflow-ui/GitPanel.tsx'), 'utf8');
    const runStart = panel.indexOf('const run = useCallback(');
    const runEnd = runStart < 0 ? -1 : panel.indexOf('const handleInit', runStart);
    const runBody = runStart >= 0 && runEnd > runStart ? panel.slice(runStart, runEnd) : '';
    check(
        'git: the panel\'s action runner is still where it was',
        runBody !== '',
        'GitPanel run() moved or changed shape; update this check to follow it',
    );
    check(
        'git: every action goes through the rewrite rule and tells the editor',
        runBody.includes('gitActionRewritesFiles(label)') && runBody.includes('onFilesChanged'),
        'run() never asks gitActionRewritesFiles or never calls onFilesChanged',
    );
    const app = readFileSync(resolve(__FRONTEND_DIR__, 'src/App.tsx'), 'utf8');
    const element = app.slice(app.indexOf('<GitPanel'), app.indexOf('/>', app.indexOf('<GitPanel')));
    check(
        'git: the editor reloads the workspace when the panel says files changed',
        element.includes('onFilesChanged={handleReloadWorkspace}'),
        `App renders ${element.trim()}`,
    );
}

// ---------------------------------------------------------------------------
// #363: a SQL Server node that takes everything from a saved connection is
// complete. Its form names the login `user`; the connection stores `username`,
// and the engine now maps one to the other, so asking for User here refused a
// run that would work.
// ---------------------------------------------------------------------------
{
    const asked = (props: Record<string, unknown>) =>
        validatePipeline([node('s', 'src.sqlserver', props)], [])
            .issues.filter(i => i.code === 'missing-required-field')
            .map(i => i.message);
    check(
        'connection: a SQL Server node on a saved connection is not asked for its user',
        asked({ connectionRef: 'prod', tableName: 'orders' }).length === 0,
        `issues: ${JSON.stringify(asked({ connectionRef: 'prod', tableName: 'orders' }))}`,
    );
    check(
        'connection: without a connection, the user is still asked for',
        asked({ host: 'db', database: 'sales', tableName: 'orders' }).some(m => m.includes("'User'")),
        `issues: ${JSON.stringify(asked({ host: 'db', database: 'sales', tableName: 'orders' }))}`,
    );
}

// ---------------------------------------------------------------------------
// SQL names that differ only in case are the same name.
//
// A SQL name becomes a DuckDB view, and DuckDB identifiers are case-insensitive:
// "Orders" and "orders" are one view, so the second replaced the first and every
// node reading "Orders" silently got the other node's rows (checked against
// DuckDB 1.5.4). The uniqueness check compared names exactly and let it through.
// ---------------------------------------------------------------------------
{
    const aliased = (id: string, alias: string) => {
        const n = node(id, 'code.sql', { sql: 'SELECT 1' });
        (n.data as Record<string, unknown>).alias = alias;
        return n;
    };
    const codes = (ns: Node<DuckleNodeData>[]) => validatePipeline(ns, []).issues.map(i => i.code);
    check(
        'sql name: two names differing only in case are a duplicate',
        codes([aliased('a', 'Orders'), aliased('b', 'orders')]).includes('duplicate-alias'),
        `issues: ${JSON.stringify(codes([aliased('a', 'Orders'), aliased('b', 'orders')]))}`,
    );
    check(
        'sql name: a name differing from another node\'s id only in case collides with it',
        codes([aliased('a', 'NODE_B'), node('node_b', 'code.sql', { sql: 'SELECT 1' })]).includes('alias-collides-with-id'),
        `issues: ${JSON.stringify(codes([aliased('a', 'NODE_B'), node('node_b', 'code.sql', { sql: 'SELECT 1' })]))}`,
    );
    check(
        'sql name: distinct names are still fine',
        !codes([aliased('a', 'orders'), aliased('b', 'customers')]).some(c => c === 'duplicate-alias' || c === 'alias-collides-with-id'),
        'distinct names were refused',
    );
    // DuckDB folds ASCII case only: "Ärger" and "ärger" are two views (1.5.4),
    // so refusing them would be a false error the engine does not raise.
    check(
        'sql name: names differing only in non-ASCII case are distinct, as in DuckDB',
        !codes([aliased('a', 'Ärger'), aliased('b', 'ärger')]).includes('duplicate-alias'),
        'refused two names DuckDB keeps apart',
    );
}

// ---------------------------------------------------------------------------
// The duplicate-context-key warning suggests placeholders that resolve.
//
// It told people to write ${context.KEY}. Nothing defines that: a namespaced
// variable is keyed by the context's NAME (${Prod.KEY}), in the editor and the
// engine alike, so following the advice left the placeholder unresolved.
// ---------------------------------------------------------------------------
{
    const repo = [context('Prod', { DB_HOST: 'prod.db' }), context('Dev', { DB_HOST: 'dev.db' })];
    const warning = validatePipeline([node('q', 'code.sql', { sql: 'SELECT 1' })], [], repo).issues.find(
        i => i.code === 'duplicate-context-key',
    );
    const suggested = [...(warning?.message ?? '').matchAll(/\$\{([^}]+)\}/g)]
        .map(m => m[1])
        .filter(k => k !== 'DB_HOST');
    const vars = buildContextVars(repo);
    check('context key: the collision is still reported', warning !== undefined, 'no duplicate-context-key warning');
    check(
        'context key: every placeholder the warning suggests resolves',
        suggested.length > 0 && suggested.every(k => Object.prototype.hasOwnProperty.call(vars, k)),
        `suggested ${JSON.stringify(suggested)}; resolvable keys include ${JSON.stringify(Object.keys(vars))}`,
    );
}

// ---------------------------------------------------------------------------
// A date offset too large for a date is left verbatim, as the engine leaves it.
//
// The editor shifted the date anyway and formatted the invalid result, so a
// typo like ${date+300000000d} became the path segment "NaN-NaN-NaN"; the engine
// panicked on the same input. A malformed offset resolves to nothing on both.
// ---------------------------------------------------------------------------
{
    const now = new Date(Date.UTC(2026, 8, 17, 12, 0, 0));
    for (const huge of ['date+300000000d', 'datetime-99999999999d', 'now+9999999999999999999h', 'time+999999999999999999999s']) {
        const got = resolveTimeBuiltin(huge, now);
        check(`date offset: ${huge} is left verbatim`, got === null, `resolved to ${JSON.stringify(got)}`);
    }
    check(
        'date offset: an ordinary offset still resolves',
        resolveTimeBuiltin('date+1d', now) === '2026-09-18',
        `date+1d resolved to ${JSON.stringify(resolveTimeBuiltin('date+1d', now))}`,
    );
}

// ---------------------------------------------------------------------------
// A cycle through a trigger link is a cycle.
//
// The engine orders a run on every edge, triggers included (a trigger says
// "after this"), and refuses a cycle among them. The editor looked for cycles
// on data edges only, so a loop closed by a trigger link validated clean and
// then failed at Run.
// ---------------------------------------------------------------------------
{
    const edge = (id: string, source: string, target: string, connectionType: string) =>
        ({ id, source, target, data: { connectionType } }) as unknown as Edge;
    const ns = [node('a', 'code.sql', { sql: 'SELECT 1' }), node('b', 'code.sql', { sql: 'SELECT * FROM input' })];
    const codesFor = (es: Edge[]) => validatePipeline(ns, es).issues.map(i => i.code);
    check(
        'cycle: a loop closed by a trigger link is refused, as the engine refuses it',
        codesFor([edge('e1', 'a', 'b', 'main'), edge('e2', 'b', 'a', 'iterate')]).includes('cycle'),
        `issues: ${JSON.stringify(codesFor([edge('e1', 'a', 'b', 'main'), edge('e2', 'b', 'a', 'iterate')]))}`,
    );
    check(
        'cycle: a trigger link that does not close a loop is fine',
        !codesFor([edge('e1', 'a', 'b', 'main'), edge('e2', 'a', 'b', 'iterate')]).includes('cycle'),
        'a parallel trigger link was reported as a cycle',
    );
}

// ---------------------------------------------------------------------------
// A deployed schedule keeps its zone and exclusion calendar.
//
// Deploy translated a schedule to the server's shape with only its trigger, so
// a Brussels 03:00 job arrived on the server's clock (usually UTC) with its
// maintenance days forgotten. The server validates and stores both keys; a
// schedule that has neither sends neither, which the server reads as "leave
// what is there alone".
// ---------------------------------------------------------------------------
{
    const cron: Schedule = {
        id: 's1',
        pipeline_id: 'p1',
        name: 'Nightly',
        enabled: true,
        kind: { type: 'cron', expr: '0 0 3 * * *' },
        timezone: 'Europe/Brussels',
        exclude: { weekdays: ['sunday'], dates: ['2026-12-25'] },
    };
    const sent = serverSchedule(cron, 'nightly');
    check(
        'deploy: the schedule\'s zone travels with it',
        sent?.timezone === 'Europe/Brussels',
        `sent ${JSON.stringify(sent)}`,
    );
    check(
        'deploy: the schedule\'s exclusion calendar travels with it',
        JSON.stringify(sent?.exclude) === JSON.stringify(cron.exclude),
        `sent ${JSON.stringify(sent)}`,
    );
    const bare = serverSchedule({ ...cron, timezone: undefined, exclude: undefined }, 'nightly');
    check(
        'deploy: a schedule with no zone or calendar sends neither key',
        bare !== null && !('timezone' in bare) && !('exclude' in bare),
        `sent ${JSON.stringify(bare)}`,
    );
}

// ---------------------------------------------------------------------------
// The web editor's Stop button asks the server to stop the run.
//
// cancelPipeline returned at once outside the desktop app, so in the web
// edition Stop sent nothing and the run went on to write its sinks. The server
// half (cancel_pipeline, scoped to the caller's own runs) is tested in serve.rs.
// ---------------------------------------------------------------------------
{
    const g = globalThis as unknown as {
        __checkLogicInvoke?: (cmd: string, args: Record<string, unknown>) => Promise<unknown>;
    };
    const asked: string[] = [];
    g.__checkLogicInvoke = async cmd => {
        asked.push(cmd);
        return { cancelled: 1 };
    };
    await cancelPipeline();
    g.__checkLogicInvoke = undefined;
    check('web stop: Stop asks the server to cancel', asked.includes('cancel_pipeline'), `asked ${JSON.stringify(asked)}`);
}

// ---------------------------------------------------------------------------
// The web Schedules dialog talks to the server's schedule store.
//
// Every schedule call returned at once outside the desktop app: the list read
// "No schedules yet" while schedules.json held some, and a save closed the form
// as if it had worked while nothing was stored. Run now has no web equivalent,
// so it says so instead of silently doing nothing.
// ---------------------------------------------------------------------------
{
    const g = globalThis as unknown as {
        __checkLogicInvoke?: (cmd: string, args: Record<string, unknown>) => Promise<unknown>;
    };
    const asked: string[] = [];
    const stored: Schedule = {
        id: 's1',
        pipeline_id: 'p1',
        name: 'Nightly',
        enabled: true,
        kind: { type: 'cron', expr: '0 0 3 * * *' },
    };
    g.__checkLogicInvoke = async cmd => {
        asked.push(cmd);
        return cmd === 'schedule_list' ? [stored] : cmd === 'schedule_upsert' ? stored : null;
    };
    const listed = await scheduleList();
    const saved = await scheduleUpsert(stored);
    await scheduleDelete('s1');
    let runNow = '';
    try {
        await scheduleRunNow('s1');
    } catch (err) {
        runNow = String(err);
    }
    g.__checkLogicInvoke = undefined;
    check('web schedules: the list comes from the server', listed.length === 1, `listed ${JSON.stringify(listed)}`);
    check('web schedules: a save reaches the server', saved?.id === 's1' && asked.includes('schedule_upsert'), `asked ${JSON.stringify(asked)}`);
    check('web schedules: a delete reaches the server', asked.includes('schedule_delete'), `asked ${JSON.stringify(asked)}`);
    check(
        'web schedules: Run now says it is not available rather than doing nothing',
        runNow.includes('serve') && !asked.includes('schedule_run_now'),
        `Run now answered ${JSON.stringify(runNow)}`,
    );
}

// ---------------------------------------------------------------------------
// Save writes before it says saved, in the web edition too.
//
// The Save button cleared the unsaved marker first, returned early outside the
// desktop app, and ignored a failed write, so a tab read as saved with nothing
// on disk. Driven through a fake web backend that can refuse writes.
// ---------------------------------------------------------------------------
{
    const g = globalThis as unknown as { fetch: typeof fetch };
    const realFetch = g.fetch;
    const written: string[] = [];
    let refuseWrites = false;
    g.fetch = (async (url: string, init?: { body?: string }) => {
        const op = String(url).replace('/api/fs/', '');
        const body = JSON.parse(init?.body ?? '{}') as { path?: string };
        if (op === 'write') {
            if (refuseWrites) return new Response('disk full', { status: 500 });
            written.push(String(body.path));
        }
        return new Response(JSON.stringify(op === 'exists' ? { exists: true } : {}), { status: 200 });
    }) as unknown as typeof fetch;
    const realError = console.error;
    console.error = () => {};
    const ok = await saveNow('/ws', 'j1', { nodes: [], edges: [] }, [], { activeJobId: 'j1' });
    refuseWrites = true;
    const refused = await saveNow('/ws', 'j1', { nodes: [], edges: [] }, [], { activeJobId: 'j1' });
    console.error = realError;
    g.fetch = realFetch;
    check(
        'save: the web edition writes the pipeline, repository and metadata',
        ok && written.some(p => p.includes('j1.json')) && written.length === 3,
        `ok=${ok}, wrote ${JSON.stringify(written)}`,
    );
    check('save: a refused write is not reported as saved', refused === false, 'reported saved');

    const app = readFileSync(resolve(__FRONTEND_DIR__, 'src/App.tsx'), 'utf8');
    const start = app.indexOf('const saveActive = useCallback(');
    const body = start < 0 ? '' : app.slice(start, app.indexOf('}, [', start));
    check(
        'save: the Save button clears the unsaved marker only after saveNow succeeded',
        body.includes('await saveNow(') && body.indexOf('dirty: false') > body.indexOf('if (ok)'),
        'handleSave clears the marker without waiting for a successful write',
    );
    check(
        'save: the Save button is not desktop-only',
        !body.includes('if (!isInTauri() || !workspacePathState) return;'),
        'handleSave still returns early in the web edition',
    );
}

// ---------------------------------------------------------------------------
// The Data Catalog can clear a field, and does not wipe what you are typing.
//
// An emptied owner or description was sent as "not given", which the engine
// reads as "leave it alone", so the old value came back. And the panel reloaded
// on every render of the editor - a run sends a stream of events, each one a
// render - and the form re-seeded from the reload, erasing half-typed text.
// ---------------------------------------------------------------------------
{
    const cleared = annotationPatch('  ', '', 'pii, ');
    check(
        'catalog: emptying owner and description clears them rather than keeping the old values',
        cleared.owner === '' && cleared.description === '',
        `sent ${JSON.stringify(cleared)}`,
    );
    check('catalog: tags still split and trim', JSON.stringify(cleared.tags) === '["pii"]', JSON.stringify(cleared.tags));
    const panel = readFileSync(resolve(__FRONTEND_DIR__, 'src/workflow-ui/CatalogPanel.tsx'), 'utf8');
    check(
        'catalog: the panel does not reload whenever its close handler is a new function',
        !panel.includes('}, [load, onClose]);'),
        'the load effect depends on onClose, which App recreates on every render',
    );
    const seed = panel.slice(panel.indexOf('// Re-seed when a different asset is selected'));
    const deps = seed.slice(seed.indexOf('}, ['), seed.indexOf(']);') + 3);
    check(
        'catalog: the form re-seeds only for a different asset',
        deps === '}, [asset.id]);',
        `the re-seed effect depends on ${deps}, which changes on every reload`,
    );
}

// ---------------------------------------------------------------------------
// A saved Google Cloud Storage connection carries what a GCS node authenticates with.
//
// The connection offered a bucket and an "Account / Project" field that nothing
// reads, and none of the HMAC key, secret, region or endpoint the engine's GCS
// secret is built from, so a node using the connection read the bucket as
// nobody. The keys the engine reads are taken from its source, so the two cannot
// drift apart quietly.
// ---------------------------------------------------------------------------
{
    const engine = readFileSync(resolve(__FRONTEND_DIR__, '../crates/duckdb-engine/src/lib.rs'), 'utf8');
    const branch = engine.slice(engine.indexOf('"gcs" => {'), engine.indexOf('"azureblob" =>'));
    const read = [...branch.matchAll(/get\("([A-Za-z]+)"\)/g)].map(m => m[1]);
    const offered = CONNECTION_TYPES.find(t => t.kind === 'gcs')?.fields ?? [];
    const missing = read.filter(k => !(offered as string[]).includes(k));
    check('gcs connection: the engine branch was found', read.includes('accessKey'), `read ${JSON.stringify(read)}`);
    check(
        'gcs connection: every credential the GCS secret reads can be saved on the connection',
        missing.length === 0,
        `the connection cannot hold ${JSON.stringify(missing)}`,
    );
    check(
        'gcs connection: no field that nothing reads',
        !(offered as string[]).includes('accountName'),
        'accountName is offered and never read for GCS',
    );
}

// ---------------------------------------------------------------------------
// The bottom panel opens Output when a run ends, not on every event of it.
//
// The editor builds a new run result for each streamed event, and the panel
// switched to Output and expanded whenever the result changed, so during a run
// Console, Problems and collapsing were all taken back on the next event.
// ---------------------------------------------------------------------------
{
    const panel = readFileSync(resolve(__FRONTEND_DIR__, 'src/workflow-ui/BottomPanel.tsx'), 'utf8');
    const start = panel.indexOf('// Auto-expand Output tab when a run finishes.');
    const effect = start < 0 ? '' : panel.slice(start, panel.indexOf(']);', start) + 3);
    check('bottom panel: the auto-open effect is still where it was', effect !== '', 'the Output auto-open effect moved');
    check(
        'bottom panel: Output opens once the run has ended, not on each streamed event',
        effect.includes('!isRunning') && effect.includes('[runResult, isRunning]'),
        `the effect reacts to every run-result change: ${effect.replace(/\s+/g, ' ')}`,
    );
}

// ---------------------------------------------------------------------------
// Autodetect's progress and error belong to the node they were for.
//
// Both were panel-wide state, so selecting another node while a detect ran, or
// after one failed, showed "Detecting..." with the button disabled, or the
// first source's connection error, on a node that had nothing to do with it.
// ---------------------------------------------------------------------------
{
    const panel = readFileSync(resolve(__FRONTEND_DIR__, 'src/workflow-ui/PropertiesPanel.tsx'), 'utf8');
    check(
        'autodetect: the running state names the node it is for',
        panel.includes('const [autodetectingFor, setAutodetectingFor] = useState<string | null>(null);'),
        'autodetecting is one flag for the whole panel',
    );
    check(
        'autodetect: the error names the node it is for, and only that node shows it',
        panel.includes('useState<{ nodeId: string; message: string } | null>(null)') &&
            panel.includes('detectError?.nodeId === selected?.id'),
        'the detect error is shown on whichever node is selected',
    );
}

// ---------------------------------------------------------------------------
// A web run the server refuses says why.
//
// The streaming run answered a refusal - a bad pipeline, a missing connection,
// a role that may not run - with only "run failed: HTTP 400", though the server
// sends the reason in the body.
// ---------------------------------------------------------------------------
{
    const g = globalThis as unknown as { fetch: typeof fetch };
    const realFetch = g.fetch;
    const answers: [number, string, string][] = [
        [400, '{"error":"bad pipeline: missing field `nodes`"}', 'missing field `nodes`'],
        [403, 'this needs the operator role; you have viewer', 'operator role'],
    ];
    for (const [status, body, reason] of answers) {
        g.fetch = (async () => new Response(body, { status })) as unknown as typeof fetch;
        const r = await runPipeline([], []);
        check(
            `web run: an HTTP ${status} refusal carries the server's reason`,
            !!r && r.status === 'error' && String(r.error).includes(reason),
            `error was ${JSON.stringify(r?.error)}`,
        );
    }
    g.fetch = realFetch;
}

// ---------------------------------------------------------------------------
// Web Settings do not say "Saved" for settings nothing stores.
//
// The web server has no command for the proxy, AI endpoint, power, memory,
// unsigned-extension or context-file settings, and the shim turns the missing
// command into a quiet success, so Save said "Saved" and nothing changed. They
// configure the machine that runs pipelines, so in the web edition they are set
// on the server, and saving one now says so.
// ---------------------------------------------------------------------------
{
    const g = globalThis as unknown as {
        __checkLogicInvoke?: (cmd: string, args: Record<string, unknown>) => Promise<unknown>;
    };
    g.__checkLogicInvoke = async () => null;
    const setters: [string, () => Promise<unknown>][] = [
        ['proxy', () => settingsSetProxy('/ws', 'http://proxy:8080')],
        ['AI endpoint', () => settingsSetAi('/ws', { baseUrl: 'http://ai', model: null, apiKey: null })],
        ['power', () => settingsSetPower('/ws', { maxConcurrentRuns: 2, memoryLimitMb: null, spillDir: null })],
        ['memory limit', () => settingsSetMemoryLimit('/ws', 512)],
        ['unsigned extensions', () => settingsSetAllowUnsigned('/ws', true)],
        ['context file', () => settingsSetContextFile('/ws', 'ctx.env')],
    ];
    for (const [what, set] of setters) {
        let refused = '';
        try {
            await set();
        } catch (err) {
            refused = String(err);
        }
        check(`web settings: saving the ${what} says it is not stored here`, refused.includes('server'), `resolved as saved`);
    }
    g.__checkLogicInvoke = undefined;
}

// ---------------------------------------------------------------------------
// A run's badges and results belong to the pipeline that ran.
//
// The canvas badges, the Output tab and the Problems count all read one run
// result, whichever pipeline's tab was open. A duplicated pipeline keeps its
// node ids, so after running the original the copy showed every node as run
// and "Run succeeded", although it never ran.
// ---------------------------------------------------------------------------
{
    const app = readFileSync(resolve(__FRONTEND_DIR__, 'src/App.tsx'), 'utf8');
    const consumers = [
        /<RunStatusContext\.Provider value=\{(\w+)\?\.nodes/,
        /<EditorTabs[\s\S]*?runResult=\{(\w+)\}/,
        /<BottomPanel[\s\S]*?runResult=\{(\w+)\}/,
    ].map(re => app.match(re)?.[1] ?? '(not found)');
    check(
        'run scope: the canvas, editor tabs and bottom panel show only the open pipeline run',
        consumers.every(name => name !== 'runResult' && name !== '(not found)') &&
            /setRunResultFor\(activeJobId\)/.test(app),
        `they read ${JSON.stringify(consumers)}`,
    );
    // Going back to the pipeline that ran shows its result again, which is not a
    // run ending: the panel must not reopen Output, and expand, on a tab switch.
    const panel = readFileSync(resolve(__FRONTEND_DIR__, 'src/workflow-ui/BottomPanel.tsx'), 'utf8');
    const start = panel.indexOf('// Auto-expand Output tab when a run finishes.');
    const effect = start < 0 ? '' : panel.slice(start, panel.indexOf(']);', start) + 3);
    check(
        'run scope: returning to a pipeline does not reopen Output for a result already shown',
        /(\w+)\.current !== runResult/.test(effect),
        `the effect reopens on every appearance of a result: ${effect.replace(/\s+/g, ' ')}`,
    );
}

// ---------------------------------------------------------------------------
// A run resolves the context first and the date builtins over the result.
//
// That is the order of the scheduler, and now of the runner and the server. The
// editor already let a context's own `date` win, but inserted a context value
// in one pass, so `OUT = exports/${date}` reached the engine as a folder
// literally named `${date}`.
// ---------------------------------------------------------------------------
{
    const sink = {
        id: 'k',
        position: { x: 0, y: 0 },
        data: { label: 'out', componentId: 'snk.csv', properties: { path: '${OUT}/orders.csv', note: '${date}' } },
    } as unknown as Node<DuckleNodeData>;
    const props = resolveForRun([sink], [context('dev', { OUT: 'exports/${date}' })])[0].data.properties ?? {};
    check(
        'run order: a date builtin inside a context value resolves',
        /^exports\/\d{4}-\d{2}-\d{2}\/orders\.csv$/.test(String(props.path)),
        `path ${String(props.path)}`,
    );
    const business = resolveForRun([sink], [context('dev', { date: '2020-01-31' })])[0].data.properties ?? {};
    check('run order: a context that defines date still wins', business.note === '2020-01-31', `note ${String(business.note)}`);
}

// ---------------------------------------------------------------------------
// A transform's output columns are what it produces, before it has ever run.
//
// Group By, the joins, Add Column, Coalesce, the window aggregate and the AI
// nodes have a schema the user may declare, and the resolver returned that
// declared schema or, with none, the input's columns - so their own column
// logic never ran. A Group By offered its input columns downstream, and a semi
// join offered the lookup's columns, which the engine's EXISTS never returns.
// ---------------------------------------------------------------------------
{
    const node = (id: string, componentId: string, properties: Record<string, unknown>, schema?: string[]) =>
        ({
            id,
            position: { x: 0, y: 0 },
            data: {
                label: id,
                componentId,
                properties,
                ...(schema ? { schema: schema.map(name => ({ name, type: 'string', nullable: true })) } : {}),
            },
        }) as unknown as Node<DuckleNodeData>;
    const orders = node('orders', 'src.csv', {}, ['id', 'region', 'amount']);
    const regions = node('regions', 'src.csv', {}, ['region', 'manager']);
    const into = (target: string): Edge[] => [
        { id: 'm', source: 'orders', target },
        { id: 'l', source: 'regions', target, targetHandle: 'lookup' },
    ];
    const out = (n: Node<DuckleNodeData>, edges: Edge[] = [{ id: 'm', source: 'orders', target: n.id }]) =>
        JSON.stringify(resolveOutputSchema(n.id, [orders, regions, n], edges).map(c => c.name));

    const grouped = out(node('g', 'xf.groupby', { groupKeys: ['region'], aggregations: [{ func: 'sum', column: 'amount', output: 'total' }] }));
    check('computed schema: Group By outputs its keys and aggregates', grouped === '["region","total"]', grouped);
    const added = out(node('a', 'xf.addcol', { name: 'flag', type: 'bool' }));
    check('computed schema: Add Column adds its column', added === '["id","region","amount","flag"]', added);
    const embedded = out(node('e', 'xf.ai.embed', { outputColumn: 'vec' }));
    check('computed schema: an AI node adds its output column', embedded === '["id","region","amount","vec"]', embedded);
    const semi = out(node('s', 'xf.semi', {}), into('s'));
    check('computed schema: a semi join keeps only the main input', semi === '["id","region","amount"]', semi);
    const joined = out(node('j', 'xf.join', {}), into('j'));
    check('computed schema: a join still has both sides', joined === '["id","region","amount","manager"]', joined);
    const ran = out(node('r', 'xf.groupby', { groupKeys: ['region'] }, ['region', 'orders']));
    check('computed schema: a schema already on the node is kept', ran === '["region","orders"]', ran);
}

// ---------------------------------------------------------------------------
// A Rename node's card says how many columns it renames.
//
// The Rename form writes `mapping` as old -> new pairs, which is what the engine
// reads, but the subtitle only counted the older `renames` / `columns` arrays, so
// a node set up in the form never showed one.
// ---------------------------------------------------------------------------
{
    const fromForm = deriveNodeSubtitle('xf.rename', {
        mapping: [
            { key: 'cust_id', value: 'customer_id' },
            { key: 'amt', value: 'amount' },
            { key: 'half', value: '' },
        ],
    });
    check('rename subtitle: the form shape is counted', fromForm === 'rename 2', `got ${fromForm}`);
    const older = deriveNodeSubtitle('xf.rename', { renames: [{ from: 'a', to: 'b' }] });
    check('rename subtitle: the older array shape still counts', older === 'rename 1', `got ${older}`);
}

// ---------------------------------------------------------------------------
// Duckie's Retry in the web editor does not claim an install that never ran.
//
// The server has no engine_install, which the web shim answers as an empty
// success, so Retry marked the AI engine ready and the next message was refused
// as desktop-only.
// ---------------------------------------------------------------------------
{
    const g = globalThis as unknown as {
        __checkLogicInvoke?: (cmd: string, args: Record<string, unknown>) => Promise<unknown>;
    };
    const asked: string[] = [];
    g.__checkLogicInvoke = async cmd => {
        asked.push(cmd);
        return null;
    };
    let outcome = 'resolved';
    try {
        await engineInstall('llamacpp');
    } catch (e) {
        outcome = e instanceof Error ? e.message : String(e);
    }
    g.__checkLogicInvoke = undefined;
    check(
        'web duckie: Retry fails with the reason instead of reporting an install',
        outcome !== 'resolved' && outcome.includes('desktop app') && asked.length === 0,
        `outcome ${JSON.stringify(outcome)}, asked ${JSON.stringify(asked)}`,
    );
}

// ---------------------------------------------------------------------------
// Connect to Claude in the web editor says why it cannot connect.
//
// duckle-mcp speaks stdio, so it runs on the computer the AI client runs on,
// which the web editor cannot reach. The server has no command for it, so the
// details came back empty and the dialog spun on "Preparing" forever.
// ---------------------------------------------------------------------------
{
    const g = globalThis as unknown as {
        __checkLogicInvoke?: (cmd: string, args: Record<string, unknown>) => Promise<unknown>;
    };
    g.__checkLogicInvoke = async () => null;
    let outcome = 'resolved';
    try {
        await mcpConnectionInfo();
    } catch (e) {
        outcome = e instanceof Error ? e.message : String(e);
    }
    g.__checkLogicInvoke = undefined;
    check(
        'web mcp: the dialog gets a reason, not an endless spinner',
        outcome !== 'resolved' && outcome.includes('duckle-mcp'),
        `outcome ${JSON.stringify(outcome)}`,
    );
}

// ---------------------------------------------------------------------------
// Build pipeline in the web editor says where a bundle is built.
//
// The server has no build command, which the web shim answers as an empty
// success, so Build showed "Building..." and then nothing at all.
// ---------------------------------------------------------------------------
{
    const g = globalThis as unknown as {
        __checkLogicInvoke?: (cmd: string, args: Record<string, unknown>) => Promise<unknown>;
    };
    const asked: string[] = [];
    g.__checkLogicInvoke = async cmd => {
        asked.push(cmd);
        return null;
    };
    let refusal = '';
    try {
        await buildBundle('/ws', 'p_7f3a', 'orders.exe', null, 'env');
    } catch (e) {
        refusal = e instanceof Error ? e.message : String(e);
    }
    g.__checkLogicInvoke = undefined;
    check(
        'web build: Build explains itself instead of doing nothing',
        refusal.includes('duckle-runner build') && refusal.includes('p_7f3a') && asked.length === 0,
        `refusal ${JSON.stringify(refusal)}, asked ${JSON.stringify(asked)}`,
    );
}

// ---------------------------------------------------------------------------
// The web History tab reads the server's run history.
//
// runHistory returned an empty list at once outside the desktop app, so the tab
// always said there was no run history.
// ---------------------------------------------------------------------------
{
    const g = globalThis as unknown as {
        __checkLogicInvoke?: (cmd: string, args: Record<string, unknown>) => Promise<unknown>;
    };
    let asked: { cmd: string; args: Record<string, unknown> } | null = null;
    g.__checkLogicInvoke = async (cmd, args) => {
        asked = { cmd, args };
        return [{ at: '2026-09-17T10:00:00Z', status: 'ok', duration_ms: 5, rows: 3, node_count: 2, trigger: 'web' }];
    };
    const records = await runHistory('/ws', 'p_7f3a');
    g.__checkLogicInvoke = undefined;
    check(
        'web history: the tab asks the server for the pipeline history',
        records.length === 1 && JSON.stringify(asked) === JSON.stringify({ cmd: 'run_history', args: { workspacePath: '/ws', pipelineId: 'p_7f3a' } }),
        `asked ${JSON.stringify(asked)}, got ${JSON.stringify(records)}`,
    );
}

// ---------------------------------------------------------------------------
// The web Backfill panel works: the server answers its commands.
//
// The server implements watermark_list, watermark_set and watermark_clear for
// this panel, but the bridge returned at once outside the desktop app and the
// panel said "Backfill is available in the desktop app".
// ---------------------------------------------------------------------------
{
    const g = globalThis as unknown as {
        __checkLogicInvoke?: (cmd: string, args: Record<string, unknown>) => Promise<unknown>;
    };
    const asked: string[] = [];
    g.__checkLogicInvoke = async cmd => {
        asked.push(cmd);
        return cmd === 'watermark_list' ? [{ nodeId: 'i', kind: 'incremental', value: '2026-01-01' }] : null;
    };
    const listed = await watermarkList('/ws', 'orders');
    await watermarkSet('/ws', 'orders', 'i', 'incremental', '2026-02-01');
    await watermarkClear('/ws', 'orders', 'i');
    g.__checkLogicInvoke = undefined;
    check('web backfill: the saved state is listed from the server', listed.length === 1, `listed ${JSON.stringify(listed)}`);
    check(
        'web backfill: setting and clearing reach the server',
        asked.includes('watermark_set') && asked.includes('watermark_clear'),
        `asked ${JSON.stringify(asked)}`,
    );
    const modal = readFileSync(resolve(__FRONTEND_DIR__, 'src/workflow-ui/BackfillModal.tsx'), 'utf8');
    check(
        'web backfill: the panel is not desktop-only',
        !modal.includes('Backfill is available in the desktop app.'),
        'the panel still turns the web edition away',
    );
}

// ---------------------------------------------------------------------------
// The Schedules dialog shows why "Run now" or "Delete" failed.
//
// Both awaited their command with no catch, so a refusal ("already running in
// this workspace, so this run was refused") was an unhandled rejection and the
// dialog showed nothing. The list view did not render the error either: only
// the edit form did.
// ---------------------------------------------------------------------------
{
    const refusal = 'orders is already running in this workspace, so this run was refused';
    let got: string | null | 'threw' = 'threw';
    try {
        got = await scheduleActionError(async () => {
            throw refusal;
        });
    } catch {
        got = 'threw';
    }
    check('schedule action: a refusal becomes the message to show', got === refusal, `got ${JSON.stringify(got)}`);
    check(
        'schedule action: success shows nothing',
        (await scheduleActionError(async () => undefined)) === null,
        'a successful action produced a message',
    );
    const modal = readFileSync(resolve(__FRONTEND_DIR__, 'src/workflow-ui/ScheduleEditorModal.tsx'), 'utf8');
    for (const handler of ['handleDelete', 'handleRunNow']) {
        const start = modal.indexOf(`const ${handler} = `);
        const body = start < 0 ? '' : modal.slice(start, modal.indexOf('};', start));
        check(
            `schedule action: ${handler} reports its failure`,
            body.includes('scheduleActionError(') && body.includes('setError('),
            `${handler} does not route its command through scheduleActionError into setError`,
        );
    }
    const list = modal.slice(modal.indexOf('<div className="schedule-list">'), modal.indexOf('schedule-add'));
    check(
        'schedule action: the list view renders the error',
        list.includes('{error ?'),
        'the error is set but only the edit form shows it',
    );
}

// #317: a declared parameter is the engine's to fill - it checks the value against
// the declaration, applies the default and treats it as a value. So the editor
// must not prompt for one as a bare placeholder, nor substitute a context of the
// same name, and a run carries the pipeline's own top-level fields.
{
    const nodes = [node('k', 'snk.csv', { path: 'out/${region}-${batch}.csv' })];
    const declared = ['region'];
    const params = discoverParams(nodes, {}, declared);
    check(
        '#317: a declared parameter is not prompted for as a bare placeholder',
        !params.includes('region') && params.includes('batch'),
        `prompted ${JSON.stringify(params)}`,
    );
    const path = String(
        resolveForRun(nodes, [context('dev', { region: 'us', batch: '7' })], undefined, undefined, undefined, declared)[0]
            .data.properties?.path,
    );
    check(
        '#317: a context does not pre-empt a declared parameter',
        path === 'out/${region}-7.csv',
        `the browser substituted it, bypassing the contract: ${path}`,
    );
    const fields = pipelineRunFields({
        formatVersion: 1, nodes: [], edges: [], viewport: { x: 1 },
        parameters: { region: { type: 'string' } }, maxRunSeconds: 60, resourcePool: 'etl',
    });
    check(
        '#317: a run carries the parameter contract, time limit and pool, and nothing else',
        JSON.stringify(fields) ===
            JSON.stringify({ formatVersion: 1, parameters: { region: { type: 'string' } }, maxRunSeconds: 60, resourcePool: 'etl' }),
        JSON.stringify(fields),
    );
}

// #317: a schedule's parameter values survive a save that does not mention them
// (the dialog of a pipeline with no declarations sends none), are replaced by one
// that does, and travel with the schedule when it is deployed to a server.
{
    const loaded = {
        id: 's1', pipeline_id: 'p', name: 'n', enabled: true,
        kind: { type: 'interval' as const, seconds: 60 }, params: { region: 'us' },
    };
    const base = { id: 's1', pipelineId: 'p', name: 'renamed', enabled: true, kind: loaded.kind };
    const kept = scheduleForSave(loaded, base);
    check('#317: a save that does not mention parameters keeps them', kept.params?.region === 'us', JSON.stringify(kept));
    const replaced = scheduleForSave(loaded, { ...base, params: { region: 'eu' } });
    check('#317: a save that sets parameters replaces them', replaced.params?.region === 'eu', JSON.stringify(replaced));
    const cleared = scheduleForSave(loaded, { ...base, params: {} });
    check('#317: an empty set clears them', JSON.stringify(cleared.params) === '{}', JSON.stringify(cleared));
    const deployed = serverSchedule(loaded, 'p');
    check(
        '#317: a deployed schedule carries its parameter values',
        JSON.stringify((deployed ?? {}).params) === JSON.stringify({ region: 'us' }),
        JSON.stringify(deployed),
    );
}

// #384: a tour card is as tall as its text and the font size make it, and on a first run
// its Next button is the only way forward. A collapsed bottom panel in a 915px window, with
// a card taller than the old fixed guess, pushed the buttons off the window.
{
    const panel = { top: 850, left: 0, width: 980, height: 60 };
    const view = { width: 980, height: 915 };
    const tip = { width: 340, height: 420 };
    const above = placeTip(panel, 'top', view, tip);
    check('#384: the card sits above the panel by its real height', above.top + tip.height + 10 === panel.top, JSON.stringify(above));
    for (const place of ['top', 'bottom', 'left', 'right'] as const) {
        const at = placeTip(panel, place, view, tip);
        check(`#384: a ${place} card stays inside the window`, at.top >= 0 && at.top + tip.height <= view.height, JSON.stringify(at));
    }
}

// The Tests tab writes the file `duckle test` reads, and that harness refuses a case that
// asserts nothing, so the tab has to know the same rule.
{
    const fresh = newCase('f', ['Test 1']);
    check('a new case is named apart from the others', fresh.name === 'Test 2', fresh.name);
    check('a new case can run as it stands', caseProblem(fresh) === null, String(caseProblem(fresh)));
    const empty = { name: 'c', expect: { node: 'f' } };
    check('a case asserting nothing is a problem', caseProblem(empty) !== null, 'no problem reported');
    check(
        'an empty list is not an assertion',
        caseProblem({ name: 'c', expect: { node: 'f', unique: [], sql: '  ' } }) !== null,
        'no problem reported',
    );
    check(
        'dummy data with no columns is said before it fails a run',
        (caseProblem({
            name: 'c',
            given: { s: { generate: { rowCount: 10, columns: [] } } },
            expect: { node: 'f', rowCount: 10 },
        }) ?? '').includes('columns'),
        'no problem reported',
    );
    const written = cleaned({ name: 'c', given: {}, expect: { node: 'f', unique: [], notNull: ['a'], sql: '' } });
    check(
        'what is written carries no empty assertions',
        JSON.stringify(written) === JSON.stringify({ name: 'c', expect: { node: 'f', notNull: ['a'] } }),
        JSON.stringify(written),
    );
    check('a suite names its pipeline from the tests folder', newSuite('orders').pipeline === '../pipelines/orders.json', newSuite('orders').pipeline);
    check('a column list is read from what a person types', JSON.stringify(listOf(' a, b ,,c ')) === JSON.stringify(['a', 'b', 'c']), 'bad list');

    const gen = (name: string, type: string) => guessGenerator({ name, type, nullable: true } as never).generator;
    for (const [name, type, want] of [
        ['id', 'int64', 'sequence'],
        ['customer_id', 'string', 'uuid'],
        ['first_name', 'string', 'person.firstName'],
        ['LastName', 'string', 'person.lastName'],
        ['customer_name', 'string', 'person.fullName'],
        ['Email', 'string', 'internet.email'],
        ['phone_number', 'string', 'phone.number'],
        ['city', 'string', 'address.city'],
        ['amount', 'decimal', 'decimal'],
        ['quantity', 'int32', 'integer'],
        ['active', 'bool', 'boolean'],
        ['created', 'timestamp', 'timestamp'],
        ['ordered_on', 'date', 'date'],
        ['notes', 'string', 'text.word'],
    ] as const) {
        check(`a column called ${name} is filled by ${want}`, gen(name, type) === want, gen(name, type));
    }
    const input = dummyInput([{ name: 'id', type: 'int64', nullable: false }]);
    check('dummy input is shaped like the source', input.generate.columns[0].generator === 'sequence', JSON.stringify(input));
}

// The Data Generator's columns are known from its form, so what is wired after it can offer
// them before anything has run.
{
    const generator = node('g', 'src.generate', {
        columns: [
            { name: 'id', generator: 'sequence' },
            { name: 'who', generator: 'person.fullName', nullPercent: '10' },
            { name: 'lat', generator: 'address.latitude' },
            { name: 'joined', generator: 'date' },
        ],
    });
    const schema = resolveOutputSchema('g', [generator], []);
    check(
        'a generator names its columns and their types',
        JSON.stringify(schema.map(c => [c.name, c.type, c.nullable]))
            === JSON.stringify([['id', 'int64', false], ['who', 'string', true], ['lat', 'float64', false], ['joined', 'date', false]]),
        JSON.stringify(schema),
    );
    const empty = validatePipeline([node('g', 'src.generate', {})], []).issues;
    check(
        'a generator with no columns is a problem the editor reports',
        empty.some(i => i.code === 'missing-required-field' && i.message.includes('Columns')),
        JSON.stringify(empty),
    );
}

// ---------------------------------------------------------------------------
// Deleting a connection says which pipelines still use it.
//
// The confirmation only said "This cannot be undone", so a connection several
// pipelines read through was deleted with nothing to say they would stop
// running. A node names its connection in any field of kind connection-ref:
// `connectionRef`, and on REST sources the HTTP transport's `transportRef` too.
// ---------------------------------------------------------------------------
{
    const restFields = (getManifest('src.rest')?.sections ?? []).flatMap(s => s.fields);
    check(
        'connection usage: src.rest still carries transportRef as a connection field',
        restFields.some(f => f.key === 'transportRef' && f.kind === 'connection-ref'),
        'the REST case below would pass for the wrong reason',
    );
    const item = (id: string, name: string, type: string, parentId = 'root'): RepoItem =>
        ({ id, name, type, parentId }) as unknown as RepoItem;
    const repo = [
        item('conns', 'Connections', 'folder'),
        item('pg', 'warehouse', 'connection', 'conns'),
        item('proxy', 'corp proxy', 'connection', 'conns'),
        item('other', 'other db', 'connection', 'conns'),
        item('p1', 'orders_etl', 'pipeline'),
        item('p2', 'api_pull', 'pipeline'),
        item('p3', 'unrelated', 'pipeline'),
        item('p4', 'nested load', 'pipeline', 'conns'),
    ];
    const data = {
        p1: { nodes: [node('a', 'src.postgres', { connectionRef: 'pg', table: 'orders' })] },
        p2: { nodes: [node('b', 'src.rest', { url: 'https://example.test', transportRef: 'proxy' })] },
        p3: { nodes: [node('c', 'src.postgres', { connectionRef: 'other' }), node('d', 'xf.filter', { connectionRef: 'pg' })] },
        p4: { nodes: [node('e', 'src.postgres', { connectionRef: 'pg' })] },
    };
    const names = (ids: string[]) => JSON.stringify(pipelinesUsingConnections(new Set(ids), repo, data));
    check('connection usage: a pipeline reading through the connection is named', names(['pg']) === '["orders_etl","nested load"]', names(['pg']));
    check('connection usage: an HTTP transport counts as a connection', names(['proxy']) === '["api_pull"]', names(['proxy']));
    check('connection usage: a property that is not a connection field does not count', !names(['pg']).includes('unrelated'), names(['pg']));
    check('connection usage: nothing uses it, nothing is named', names(['other', 'p3']) === '[]', names(['other', 'p3']));
    check(
        'connection usage: a pipeline deleted along with the connection is not named',
        names(['conns', 'pg', 'proxy', 'other', 'p4']) === '["orders_etl","api_pull","unrelated"]',
        names(['conns', 'pg', 'proxy', 'other', 'p4']),
    );
    check('connection usage: deleting a pipeline names nothing', names(['p1']) === '[]', names(['p1']));
}

// ---------------------------------------------------------------------------
// Align and distribute, as Hop and Talend have them.
//
// Aligning lines the selection up on its outermost edge; distributing leaves the
// outer two where they are and spaces the rest so every gap is equal. Only the
// nodes that move are returned, so nothing else is written.
// ---------------------------------------------------------------------------
{
    const box = (id: string, x: number, y: number, width: number, height: number) => ({
        id,
        position: { x, y },
        measured: { width, height },
    });
    const a = box('a', 10, 0, 100, 50);
    const b = box('b', 50, 30, 80, 40);
    const c = box('c', 400, 200, 100, 60);
    const moved = (mode: Parameters<typeof arrange>[1], nodes: ReturnType<typeof box>[]) =>
        JSON.stringify(arrange(nodes, mode).map(m => [m.id, m.position.x, m.position.y]));
    check('align: left lines up on the leftmost edge', moved('left', [a, b]) === '[["b",10,30]]', moved('left', [a, b]));
    check('align: right lines up on the rightmost edge', moved('right', [a, b]) === '[["a",30,0]]', moved('right', [a, b]));
    check('align: top lines up on the topmost edge', moved('top', [a, b]) === '[["b",50,0]]', moved('top', [a, b]));
    check('align: bottom lines up on the lowest edge', moved('bottom', [a, b]) === '[["a",10,20]]', moved('bottom', [a, b]));
    const m = box('m', 150, 0, 50, 10);
    const left = box('l', 0, 0, 100, 10);
    const right = box('r', 400, 0, 100, 10);
    check(
        'distribute: horizontally makes every gap equal and keeps the ends',
        moved('distribute-x', [right, m, left]) === '[["m",225,0]]',
        moved('distribute-x', [right, m, left]),
    );
    const top = box('t', 0, 0, 10, 100);
    const mid = box('mid', 0, 120, 10, 20);
    const low = box('low', 0, 300, 10, 100);
    check(
        'distribute: vertically makes every gap equal and keeps the ends',
        moved('distribute-y', [low, top, mid]) === '[["mid",0,190]]',
        moved('distribute-y', [low, top, mid]),
    );
    check('distribute: two nodes have nothing to distribute', moved('distribute-x', [a, c]) === '[]', moved('distribute-x', [a, c]));
}

// ---------------------------------------------------------------------------
// A canvas note is part of the undo history.
//
// The history keeps whole snapshots and records a step only when its key changes.
// A note left out of the key would never be a step of its own, and the next undo
// would take it back together with whatever came before.
// ---------------------------------------------------------------------------
{
    const timers = new Map<number, () => void>();
    let nextTimer = 0;
    const timer = (fn: () => void) => {
        const id = nextTimer++;
        timers.set(id, fn);
        return () => {
            timers.delete(id);
        };
    };
    const settle = () => {
        for (const [id, fn] of [...timers]) {
            timers.delete(id);
            fn();
        }
    };
    const withNote = (text: string | null, x = 0): CanvasSnapshot =>
        ({
            nodes: [],
            edges: [],
            notes: text === null ? [] : [{ id: 'note1', type: 'note', position: { x, y: 0 }, data: { text } }],
        }) as unknown as CanvasSnapshot;
    const steps = (from: CanvasSnapshot, to: CanvasSnapshot) => {
        timers.clear();
        const h = new UndoHistory('a', from, timer, () => {});
        h.observe('a', from);
        h.observe('a', to);
        settle();
        return h.undo() === from;
    };
    check('undo: adding a note is a step', steps(withNote(null), withNote('')), 'undo did not take the note away');
    check('undo: writing in a note is a step', steps(withNote(''), withNote('check the VAT rule')), 'undo did not restore the text');
    check('undo: moving a note is a step', steps(withNote('x', 0), withNote('x', 200)), 'undo did not move the note back');
}

// ---------------------------------------------------------------------------
// Generated pipeline documentation, as Talend's "Generate Doc as HTML".
//
// One page a person can read without Duckle: every step in the order it runs,
// what it is set to, the columns it produces, the notes, and what it needs
// from outside. It is meant to be shared, so nothing secret goes into it: a
// value under a credential key, an auth header, a password inside a URL or a
// connection string, a secret context variable or parameter, or any setting
// of a saved connection beyond its name and kind. A secret written as a
// reference, ${ENV:NAME}, says where it comes from and stays.
// ---------------------------------------------------------------------------
{
    const step = (id: string, componentId: string, label: string, properties: Record<string, unknown>, x: number, extra: Record<string, unknown> = {}) =>
        ({ id, position: { x, y: 0 }, data: { label, componentId, properties, ...extra } }) as unknown as Node<DuckleNodeData>;
    const nodes = [
        step('k1', 'snk.parquet', 'Write paid', { path: 'out/${api_token}/paid.parquet', compression: 'snappy', compressionLevel: 9 }, 900),
        step('s1', 'src.csv', 'Read orders', { path: 'data/orders.csv' }, 0, { schema: [{ name: 'order_id', type: 'int64', nullable: false }] }),
        step('t1', 'xf.filter', 'Keep paid', { predicate: "status = 'paid' AND note <> '<b>x</b>' AND code = '${ref_code}'", apiSecret: 'undeclared-secret-222' }, 300),
        step('r1', 'src.rest', 'Pull rates', {
            url: 'https://etl:s3cr3t-in-url@api.example.test/rates?day=${batch_date}',
            authToken: 'tok-literal-123',
            headers: [{ key: 'Authorization', value: 'Bearer hdr-secret-456' }, { key: 'Accept', value: 'application/json' }],
            connectionRef: 'c_api',
        }, 0),
        step('p1', 'src.postgres', 'Load dims', {
            connectionRef: 'c_pg',
            password: '${ENV:PG_PASSWORD}',
            connString: 'host=db user=etl password=connstr-pw-789',
            tableName: 'dims',
        }, 0, { disabled: true }),
    ];
    const edges = [
        { id: 'e1', source: 's1', target: 't1', data: { connectionType: 'main' } },
        { id: 'e2', source: 't1', target: 'k1', data: { connectionType: 'main' } },
    ];
    const repo = [
        { id: 'p', name: 'orders & refunds', type: 'pipeline', parentId: 'root' },
        { id: 'c_api', name: 'rates api', type: 'connection', parentId: 'root', payload: { kind: 'rest', token: 'conn-token-000' } },
        { id: 'c_pg', name: 'warehouse', type: 'connection', parentId: 'root', payload: { kind: 'postgres', host: 'db.internal', password: 'conn-pass-321' } },
        {
            id: 'ctx',
            name: 'prod',
            type: 'context',
            parentId: 'root',
            payload: {
                variables: [
                    { key: 'batch_date', value: '2026-01-01' },
                    { key: 'api_token', value: 'ctx-secret-654', secret: true },
                    { key: 'ref_code', value: 'ctx-secret-777', secret: true },
                ],
            },
        },
    ] as unknown as RepoItem[];
    const html = pipelineDocument({
        name: 'orders & refunds',
        pipeline: {
            nodes,
            edges,
            notes: [{ id: 'note1', type: 'note', position: { x: 0, y: 300 }, data: { text: 'Paid orders only; ask finance first.' } }],
            parameters: {
                region: { type: 'string', default: 'eu', description: 'Which region to load' },
                dbpass: { type: 'secret', default: 'param-secret-111' },
            },
        },
        repo,
        contextId: 'ctx',
        generatedAt: new Date('2026-11-06T08:00:00Z'),
    });
    const has = (s: string) => html.includes(s);
    check('doc: named in the title and the heading, escaped', has('<title>orders &amp; refunds') && has('<h1>orders &amp; refunds</h1>'), html.slice(0, 300));
    // A step's own section: its name also appears in the links of the steps around it.
    const section = (id: string) => {
        const from = html.indexOf(`id="step-${id}"`);
        const to = html.indexOf('<section', from + 1);
        return from < 0 ? '' : html.slice(from, to < 0 ? undefined : to);
    };
    const at = (id: string) => html.indexOf(`id="step-${id}"`);
    check(
        'doc: steps are listed in the order they run',
        at('s1') >= 0 && at('s1') < at('t1') && at('t1') < at('k1'),
        JSON.stringify([at('s1'), at('t1'), at('k1')]),
    );
    const csvStep = section('s1');
    check('doc: a setting shows under its form label', csvStep.includes('>Path<') && csvStep.includes('data/orders.csv'), csvStep.slice(0, 400));
    check('doc: a step lists the columns it produces', csvStep.includes('order_id') && csvStep.includes('int64'), csvStep.slice(0, 400));
    check('doc: what a pipeline says is escaped, not run', has('&lt;b&gt;x&lt;/b&gt;') && !has('<b>x</b>'), 'raw HTML from a property reached the page');
    for (const secret of ['tok-literal-123', 'hdr-secret-456', 's3cr3t-in-url', 'connstr-pw-789', 'undeclared-secret-222', 'conn-token-000', 'conn-pass-321', 'db.internal', 'ctx-secret-654', 'ctx-secret-777', 'param-secret-111']) {
        check(`doc: ${secret} stays out of the page`, !has(secret), 'a secret reached a page meant to be shared');
    }
    check('doc: a secret given as a reference says where it comes from', has('${ENV:PG_PASSWORD}'), 'the reference was hidden too');
    check('doc: a header that is not a credential stays readable', has('application/json'), 'the Accept header was withheld');
    check('doc: a setting the form hides is not shown', !has('Compression level'), 'a stale compressionLevel was documented under snappy');
    check('doc: connections are named, with their kind', has('rates api') && has('warehouse') && has('postgres'), 'connection names or kinds missing');
    check('doc: a context variable it reads is listed with its value', has('batch_date') && has('2026-01-01'), 'batch_date missing');
    check('doc: a secret context variable is named but not shown', has('api_token'), 'api_token missing');
    check('doc: declared parameters are described', has('region') && has('Which region to load') && has('dbpass'), 'parameters missing');
    check('doc: the notes are on the page', has('Paid orders only; ask finance first.'), 'the note is missing');
    const dims = section('p1');
    check('doc: a disabled step says so', /disabled/i.test(dims), dims.slice(0, 300));
    check('doc: an enabled step does not', !/disabled/i.test(section('s1')), section('s1').slice(0, 300));
    check('doc: the diagram draws every step', (html.match(/<rect class="node/g) ?? []).length === 5 && has('<svg'), String((html.match(/<rect class="node/g) ?? []).length));
    check('doc: nothing renders as undefined', !/undefined|\[object Object\]|NaN/.test(html), (html.match(/.{40}(undefined|\[object Object\]|NaN).{40}/) ?? [''])[0]);
}

// A pipeline file can be written by hand, by an agent or a script: a step with no
// label, a note with no text. Its documentation still comes out, and still reads.
{
    let page = '';
    let threw = '';
    try {
        page = pipelineDocument({
            name: 'handwritten',
            pipeline: {
                nodes: [{ id: 'only', position: { x: 0, y: 0 }, data: { componentId: 'xf.filter', properties: {} } }],
                edges: [],
                notes: [{ id: 'n', type: 'note', position: { x: 0, y: 200 }, data: {} }],
            },
            repo: [],
            generatedAt: new Date('2026-11-06T08:00:00Z'),
        });
    } catch (e) {
        threw = String(e);
    }
    check('doc: a hand-written pipeline still documents', !threw && page.includes('id="step-only"'), threw || page.slice(0, 200));
    check('doc: a step with no label goes by its id', page.includes('1. only') && !/undefined/.test(page), (page.match(/.{40}undefined.{40}/) ?? [''])[0]);
}

// ---------------------------------------------------------------------------
// #372: the map reads each shape from a preview row as WKT, the way DuckDB
// prints a GEOMETRY. Every form below was printed by DuckDB 2.0 itself: Z, M and
// ZM coordinates, EMPTY, MULTIPOINT without inner parentheses, holes, and
// collections. A value that is not a shape - a masked one, a typo - is refused
// so the map can count it instead of drawing something wrong.
// ---------------------------------------------------------------------------
{
    const cases: [string, unknown][] = [
        ['POINT (1.5 2)', { type: 'Point', coordinates: [1.5, 2] }],
        ['point(1 2)', { type: 'Point', coordinates: [1, 2] }],
        ['POINT Z (1 2 3)', { type: 'Point', coordinates: [1, 2] }],
        ['POINT ZM (1 2 3 4)', { type: 'Point', coordinates: [1, 2] }],
        ['POINT (1e3 -2.5E-1)', { type: 'Point', coordinates: [1000, -0.25] }],
        ['POINT (1 2 3)', { type: 'Point', coordinates: [1, 2] }],
        ['LINESTRING M (0 0 5, 1 1 6)', { type: 'LineString', coordinates: [[0, 0], [1, 1]] }],
        [
            'POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 3 2, 3 3, 2 2))',
            {
                type: 'Polygon',
                coordinates: [
                    [[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]],
                    [[2, 2], [3, 2], [3, 3], [2, 2]],
                ],
            },
        ],
        ['MULTIPOINT (1 2, 3 4)', { type: 'MultiPoint', coordinates: [[1, 2], [3, 4]] }],
        ['MULTIPOINT ((1 2), (3 4))', { type: 'MultiPoint', coordinates: [[1, 2], [3, 4]] }],
        [
            'MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))',
            { type: 'MultiLineString', coordinates: [[[0, 0], [1, 1]], [[2, 2], [3, 3]]] },
        ],
        [
            'MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)), ((5 5, 6 5, 6 6, 5 5)))',
            {
                type: 'MultiPolygon',
                coordinates: [[[[0, 0], [1, 0], [1, 1], [0, 0]]], [[[5, 5], [6, 5], [6, 6], [5, 5]]]],
            },
        ],
        [
            'GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 1))',
            {
                type: 'GeometryCollection',
                geometries: [
                    { type: 'Point', coordinates: [1, 2] },
                    { type: 'LineString', coordinates: [[0, 0], [1, 1]] },
                ],
            },
        ],
        ['POINT EMPTY', { type: 'GeometryCollection', geometries: [] }],
        ['GEOMETRYCOLLECTION EMPTY', { type: 'GeometryCollection', geometries: [] }],
        // DuckDB writes an empty member of a multi-shape in place, as EMPTY.
        ['MULTIPOINT (EMPTY, 1 2)', { type: 'MultiPoint', coordinates: [[1, 2]] }],
        ['MULTILINESTRING (EMPTY, (0 0, 1 1))', { type: 'MultiLineString', coordinates: [[[0, 0], [1, 1]]] }],
        [
            'MULTIPOLYGON (EMPTY, ((0 0, 1 0, 1 1, 0 0)))',
            { type: 'MultiPolygon', coordinates: [[[[0, 0], [1, 0], [1, 1], [0, 0]]]] },
        ],
        [
            'GEOMETRYCOLLECTION (POINT EMPTY, POINT (1 2))',
            {
                type: 'GeometryCollection',
                geometries: [
                    { type: 'GeometryCollection', geometries: [] },
                    { type: 'Point', coordinates: [1, 2] },
                ],
            },
        ],
    ];
    for (const [text, want] of cases) {
        const got = parseWkt(text);
        check(`wkt: ${text}`, JSON.stringify(got) === JSON.stringify(want), `read as ${JSON.stringify(got)}`);
    }
    const notShapes: unknown[] = [
        '***',
        '',
        'POINT (1)',
        'POINT (1 2',
        'POINT (a b)',
        'CIRCLE (1 2)',
        'POINT (1 2) and more',
        'POINT (Infinity 2)',
        'POINT (1e400 2)',
        42,
        null,
        undefined,
        { x: 1 },
    ];
    for (const bad of notShapes) {
        const got = parseWkt(bad);
        check(`wkt: ${JSON.stringify(bad) ?? 'undefined'} is not a shape`, got === null, `read as ${JSON.stringify(got)}`);
    }
}

// ---------------------------------------------------------------------------
// #372: which columns get a map, the CRS a column's DuckDB type names, and how
// a preview's rows become features under the feature limit.
// ---------------------------------------------------------------------------
{
    const offered = geometryColumns([
        { name: 'id', type: 'int64' },
        { name: 'geom', type: 'geometry' },
        { name: 'wkt', type: 'string' },
        { name: 'g2', type: 'GEOMETRY' },
    ]);
    check('map: only GEOMETRY columns are offered', JSON.stringify(offered) === '["geom","g2"]', JSON.stringify(offered));
    const crs: [string | undefined, string | null][] = [
        ["GEOMETRY('EPSG:4326')", 'EPSG:4326'],
        ["GEOMETRY('OGC:CRS84')", 'OGC:CRS84'],
        ['GEOMETRY', null],
        ['VARCHAR', null],
        [undefined, null],
    ];
    for (const [t, want] of crs) {
        check(`map: the CRS of ${t}`, crsOfType(t) === want, `read ${JSON.stringify(crsOfType(t))}`);
    }

    const rows = [
        { id: 0, g: 'POINT (1 2)' },
        { id: 1, g: null },
        { id: 2, g: '***' },
        { id: 3, g: 'POINT EMPTY' },
        { id: 4, g: 'LINESTRING (0 0, 4 3)' },
        { id: 5, g: 'POINT (9 9)' },
        { id: 6, g: 'POINT (8 8)' },
    ];
    const two = collectFeatures(rows, 'g', 2);
    const rowsOf = (s: { features: { row: number }[] }) => JSON.stringify(s.features.map(f => f.row));
    check('map: a feature keeps the row it came from', rowsOf(two) === '[0,4]', rowsOf(two));
    check(
        'map: a null, a masked value and an empty shape are each counted, not drawn',
        two.nulls === 1 && two.unreadable === 1 && two.empty === 1,
        JSON.stringify(two),
    );
    check('map: rows past the feature limit are counted as left out', two.capped === 2, JSON.stringify(two));
    const all = collectFeatures(rows, 'g', 1000);
    check('map: under the limit nothing is left out', all.features.length === 4 && all.capped === 0, JSON.stringify(all));
    const box = boundsOf(all.features);
    check(
        'map: the extent covers every drawn coordinate',
        JSON.stringify(box) === JSON.stringify({ minX: 0, minY: 0, maxX: 9, maxY: 9 }),
        JSON.stringify(box),
    );
    check('map: nothing to draw has no extent', boundsOf([]) === null, JSON.stringify(boundsOf([])));
}

// ---------------------------------------------------------------------------
// #372: fitting, zooming and reading coordinates back. No basemap is drawn, so
// the shapes themselves have to come out the right way up, the right shape, and
// in the right place under the cursor.
// ---------------------------------------------------------------------------
{
    const lonLat = { minX: -10, minY: 35, maxX: 30, maxY: 60 };
    const metres = { minX: 500000, minY: 4000000, maxX: 600000, maxY: 4100000 };
    check('map: EPSG:4326 is geographic whatever the numbers', isGeographic('EPSG:4326', metres), 'read as projected');
    check('map: OGC:CRS84 is geographic', isGeographic('OGC:CRS84', lonLat), 'read as projected');
    check('map: a projected CRS is not, even over small numbers', !isGeographic('EPSG:3857', { minX: 1, minY: 1, maxX: 2, maxY: 2 }), 'read as geographic');
    check('map: with no CRS, degree-sized numbers read as longitude/latitude', isGeographic(null, lonLat), 'read as projected');
    check('map: with no CRS, metre-sized numbers do not', !isGeographic(null, metres), 'read as geographic');

    const near = (a: number, b: number) => Math.abs(a - b) < 1e-9;
    const v = fitView({ minX: 0, minY: 0, maxX: 4, maxY: 3 }, 100, 100, 10, false);
    const corners = ([[0, 0], [4, 0], [4, 3], [0, 3]] as [number, number][]).map(p => toScreen(v, p));
    const xs = corners.map(c => c[0]);
    check(
        'map: fitting keeps every corner inside the padding',
        corners.every(([x, y]) => x >= 10 - 1e-9 && x <= 90 + 1e-9 && y >= 10 - 1e-9 && y <= 90 + 1e-9),
        JSON.stringify(corners),
    );
    check('map: fitting fills the tighter side', near(Math.max(...xs) - Math.min(...xs), 80), JSON.stringify(corners));
    check('map: north is up', toScreen(v, [0, 3])[1] < toScreen(v, [0, 0])[1], JSON.stringify(corners));
    const north = fitView({ minX: 0, minY: 59, maxX: 2, maxY: 61 }, 100, 100, 10, true);
    check(
        'map: a degree of longitude is drawn shorter at 60 degrees north',
        near(north.xStretch, Math.cos(Math.PI / 3)),
        JSON.stringify(north),
    );
    const one = fitView({ minX: 5, minY: 5, maxX: 5, maxY: 5 }, 100, 80, 10, false);
    const at = toScreen(one, [5, 5]);
    check(
        'map: a single point is centred, not divided by zero',
        Number.isFinite(one.scale) && near(at[0], 50) && near(at[1], 40),
        JSON.stringify({ one, at }),
    );
    const zoomed = zoomAt(v, 2, 30, 40);
    const before = toMap(v, [30, 40]);
    const after = toMap(zoomed, [30, 40]);
    check('map: zooming keeps the point under the cursor', near(before[0], after[0]) && near(before[1], after[1]), JSON.stringify({ before, after }));
    check('map: zooming in doubles the scale', near(zoomed.scale, v.scale * 2), JSON.stringify(zoomed));
    const back = toMap(v, toScreen(v, [1.25, 2.5]));
    check('map: screen and map coordinates convert back exactly', near(back[0], 1.25) && near(back[1], 2.5), JSON.stringify(back));

    const mixed = parseWkt(
        'GEOMETRYCOLLECTION (POINT (11 12), LINESTRING (10 10, 13 14), POLYGON ((10 10, 14 10, 14 13, 10 10), (11 11, 12 11, 12 12, 11 11)))',
    );
    const parts = mixed ? pathParts(mixed, 10, 10) : { points: '', lines: '', polygons: '' };
    check(
        'map: each member of a collection is drawn in the layer for its kind',
        parts.points !== '' && parts.lines !== '' && parts.polygons !== '',
        JSON.stringify(parts),
    );
    check('map: shapes are drawn relative to the origin given', parts.points.startsWith('M1 2'), parts.points);
    check('map: each ring of a polygon is closed', (parts.polygons.match(/Z/g) ?? []).length === 2, parts.polygons);
}

// ---------------------------------------------------------------------------
// #372: the map's Load asks run-to-here to keep more rows for the node it maps.
// The web editor sends the number with the run; the server half is tested in
// serve.rs and the desktop command mirrors it.
// ---------------------------------------------------------------------------
{
    const g = globalThis as unknown as { fetch: typeof fetch };
    const realFetch = g.fetch;
    const seen: { body?: Record<string, unknown> } = {};
    g.fetch = (async (url: string, init?: { body?: string }) => {
        if (String(url) === '/api/run_stream') seen.body = JSON.parse(init?.body ?? '{}') as Record<string, unknown>;
        const done = { status: 'ok', duration_ms: 1, nodes: {}, preview: [] };
        return new Response(`event: result\ndata: ${JSON.stringify(done)}\n\n`, { status: 200 });
    }) as unknown as typeof fetch;
    const result = await runPipelinePartial([], [], 'f', undefined, 'p1', '/ws', 'P', { previewRows: 250 });
    g.fetch = realFetch;
    check(
        'map: run-to-here sends the rows the map asked for',
        seen.body?.previewRows === 250 && seen.body?.targetNodeId === 'f',
        JSON.stringify(seen.body),
    );
    check('map: and still reads the run back', result?.status === 'ok', JSON.stringify(result));
}

// ---------------------------------------------------------------------------
// Canvas groups, as Talend, Hop and NiFi have them: a named frame around steps
// that moves and collapses with them. The frame is never stored, only drawn
// around wherever the steps are, so it cannot drift from them.
// ---------------------------------------------------------------------------
{
    const at = (id: string, x: number, y: number, size?: { width: number; height: number }) =>
        ({ id, position: { x, y }, data: {}, ...(size ? { measured: size } : {}) }) as unknown as Node;
    // A is measured; B has not been yet and is taken as 300 x 160.
    const nodes = [at('a', 100, 100, { width: 200, height: 100 }), at('b', 400, 250), at('c', 900, 0)];
    const g: PipelineGroup = { id: 'g', label: 'Extract', members: ['a', 'b'] };
    check(
        'groups: a frame holds its steps with room for its header',
        JSON.stringify(frameRect(g, nodes)) === JSON.stringify({ x: 76, y: 40, width: 648, height: 394 }),
        JSON.stringify(frameRect(g, nodes)),
    );
    check(
        'groups: a collapsed frame keeps its corner and shrinks to a card',
        JSON.stringify(frameRect({ ...g, collapsed: true }, nodes)) === JSON.stringify({ x: 76, y: 40, width: 240, height: 64 }),
        JSON.stringify(frameRect({ ...g, collapsed: true }, nodes)),
    );
    check('groups: a group none of whose steps are on the canvas has no frame', frameRect({ ...g, members: ['gone'] }, nodes) === null, 'drew a frame');
    check(
        'groups: a frame fits a step at the size the canvas measured it',
        JSON.stringify(frameRect({ ...g, members: ['a'] }, nodes)) === JSON.stringify({ x: 76, y: 40, width: 248, height: 184 }),
        JSON.stringify(frameRect({ ...g, members: ['a'] }, nodes)),
    );

    const moved = memberMoves(g, nodes, { x: 176, y: 90 }, new Set());
    check(
        'groups: dragging a frame moves every step in it by the same amount',
        JSON.stringify(moved) === JSON.stringify([{ id: 'a', position: { x: 200, y: 150 } }, { id: 'b', position: { x: 500, y: 300 } }]),
        JSON.stringify(moved),
    );
    const once = memberMoves(g, nodes, { x: 176, y: 90 }, new Set(['a']));
    check(
        'groups: a step already being dragged with the frame is not moved twice',
        JSON.stringify(once.map(m => m.id)) === '["b"]',
        JSON.stringify(once),
    );

    const two: PipelineGroup[] = [g, { id: 'h', label: 'Load', color: 'blue', members: ['c'] }];
    const regrouped = groupNodes(two, ['b', 'c'], 'Clean', 'k');
    check(
        'groups: grouping takes the steps out of the groups they were in',
        JSON.stringify(regrouped.map(x => [x.id, x.members])) === JSON.stringify([['g', ['a']], ['k', ['b', 'c']]]),
        JSON.stringify(regrouped),
    );
    check(
        'groups: a new group takes a colour no other group has',
        groupNodes([{ ...g, color: 'orange' }], ['c'], 'New', 'n').find(x => x.id === 'n')?.color === 'blue',
        JSON.stringify(groupNodes([{ ...g, color: 'orange' }], ['c'], 'New', 'n')),
    );
    check('groups: grouping nothing changes nothing', groupNodes(two, [], 'x', 'y') === two, 'made a group of nothing');
    const out = ungroupNode(two, 'c');
    check('groups: taking out a group\'s last step drops the group', JSON.stringify(out.map(x => x.id)) === '["g"]', JSON.stringify(out));

    const kept = new Set(['a', 'b', 'c']);
    check('groups: pruning with every step there returns the same groups', pruneGroups(two, kept) === two, 'copied unchanged groups');
    const pruned = pruneGroups(two, new Set(['a']));
    check(
        'groups: a deleted step leaves its group, and an emptied group goes',
        JSON.stringify(pruned.map(x => [x.id, x.members])) === JSON.stringify([['g', ['a']]]),
        JSON.stringify(pruned),
    );

    const folded: PipelineGroup[] = [{ ...g, collapsed: true }];
    const hidden = hiddenSteps(folded);
    check('groups: the steps of a collapsed group are hidden, and only those', hidden.get('a') === 'g' && hidden.get('b') === 'g' && !hidden.has('c'), JSON.stringify([...hidden]));
    const e = (id: string, source: string, target: string) =>
        ({ id, source, target, sourceHandle: 'main', targetHandle: 'main', data: { connectionType: 'main' } }) as Edge;
    const shown = edgesForDisplay([e('inside', 'a', 'b'), e('out', 'b', 'c'), e('in', 'c', 'a'), e('free', 'c', 'd')], folded);
    const byId = new Map(shown.map(x => [x.id, x]));
    check('groups: a link inside a collapsed group is not drawn', !byId.has('inside') && !byId.has('group-edge:inside'), JSON.stringify(shown.map(x => x.id)));
    const outward = byId.get('group-edge:out');
    check(
        'groups: a link leaving a collapsed group is drawn from its frame',
        outward?.source === 'g' && outward.sourceHandle === 'group-out' && outward.target === 'c' && outward.targetHandle === 'main',
        JSON.stringify(outward),
    );
    const inward = byId.get('group-edge:in');
    check(
        'groups: a link entering a collapsed group is drawn to its frame',
        inward?.target === 'g' && inward.targetHandle === 'group-in' && inward.source === 'c',
        JSON.stringify(inward),
    );
    check('groups: a link drawn to a frame cannot be selected or deleted as if it were the real one', outward?.selectable === false && outward?.deletable === false, JSON.stringify(outward));
    check('groups: links outside any collapsed group are untouched', byId.get('free') !== undefined, JSON.stringify(shown.map(x => x.id)));
    const open = [e('inside', 'a', 'b')];
    check('groups: an expanded group changes no link', edgesForDisplay(open, [g]) === open, 'rewrote links of an open group');

    // A group is a step of the undo history of its own.
    const timers = new Map<number, () => void>();
    let next = 0;
    const timer = (fn: () => void) => {
        const id = next++;
        timers.set(id, fn);
        return () => {
            timers.delete(id);
        };
    };
    const withGroups = (groups: PipelineGroup[]): CanvasSnapshot =>
        ({ nodes: [], edges: [], groups }) as unknown as CanvasSnapshot;
    const isStep = (from: CanvasSnapshot, to: CanvasSnapshot) => {
        timers.clear();
        const h = new UndoHistory('a', from, timer, () => {});
        h.observe('a', from);
        h.observe('a', to);
        for (const [id, fn] of [...timers]) {
            timers.delete(id);
            fn();
        }
        return h.undo() === from;
    };
    check('groups: making a group is an undo step', isStep(withGroups([]), withGroups([g])), 'undo did not take the group away');
    check('groups: renaming a group is an undo step', isStep(withGroups([g]), withGroups([{ ...g, label: 'Read' }])), 'undo did not restore the name');
    check('groups: collapsing a group is an undo step', isStep(withGroups([g]), withGroups([{ ...g, collapsed: true }])), 'undo did not expand it');
}

// ---------------------------------------------------------------------------
// After a run, a link shows the rows that went down it. A link from `main`
// carries its step's own preview; a reject, filter or switch-case port sends
// other rows, which the run reports per port. A breakpoint is a condition on a
// link, and setting or clearing one must leave the rest of the link alone.
// ---------------------------------------------------------------------------
{
    const own = { node_id: 'f', columns: [], rows: [{ id: 1 }] };
    const refused = { node_id: 'f', port: 'reject', columns: [], rows: [{ id: 2 }] };
    const result = {
        status: 'ok',
        duration_ms: 1,
        nodes: {},
        preview: [own, { node_id: 'k', columns: [], rows: [] }],
        link_previews: [refused],
    } as unknown as RunResult;
    check('links: an edge with no handle leaves from main', linkPort({ sourceHandle: undefined }) === 'main' && linkPort({ sourceHandle: 'reject' }) === 'reject', 'wrong port');
    check('links: a main link shows its step\'s own rows', linkRows(result, { source: 'f', sourceHandle: 'main' }) === own, JSON.stringify(linkRows(result, { source: 'f', sourceHandle: 'main' })));
    check('links: a reject link shows the rows its port sent', linkRows(result, { source: 'f', sourceHandle: 'reject' }) === refused, JSON.stringify(linkRows(result, { source: 'f', sourceHandle: 'reject' })));
    check('links: a port the run kept nothing for has no rows', linkRows(result, { source: 'f', sourceHandle: 'case_1' }) === null, 'invented rows');
    check('links: before any run there are no rows', linkRows(null, { source: 'f', sourceHandle: 'main' }) === null, 'invented rows');

    const edges = [
        { id: 'e1', source: 'f', target: 'k', data: { connectionType: 'main', label: 'paid', condition: 'x > 1' } },
        { id: 'e2', source: 'k', target: 'z', data: { connectionType: 'main' } },
    ] as Edge[];
    const set = withBreakpoint(edges, 'e1', '  amount < 0 ');
    const data = set[0].data as Record<string, unknown>;
    check(
        'links: a breakpoint is set on its link, trimmed, and the rest of the link is kept',
        data.breakpoint === 'amount < 0' && data.label === 'paid' && data.condition === 'x > 1' && data.connectionType === 'main',
        JSON.stringify(data),
    );
    check('links: the other links are untouched', set[1] === edges[1], 'rewrote another link');
    const cleared = withBreakpoint(set, 'e1', '   ');
    check(
        'links: a blank condition takes the breakpoint off',
        !('breakpoint' in (cleared[0].data as Record<string, unknown>)) && (cleared[0].data as Record<string, unknown>).label === 'paid',
        JSON.stringify(cleared[0].data),
    );
}

// ---------------------------------------------------------------------------
// The recycle bin: a removed item keeps its file and its place in the tree, and
// what was removed together goes back together.
// ---------------------------------------------------------------------------
{
    const at = (h: number) => `2026-11-12T${String(h).padStart(2, '0')}:00:00.000Z`;
    const folder = { id: 'f1', name: 'Sales', type: 'folder', parentId: 'root' };
    const pipe = { id: 'p1', name: 'Orders', type: 'pipeline', parentId: 'f1' };
    const dive = { id: 'd1', name: 'Look', type: 'dive', parentId: 'root', payload: { big: true } };
    const a = trashEntry(folder, '100', at(10), null, null);
    const b = trashEntry(pipe, '100', at(10), 'pipelines/p1.json', '{"nodes":[]}');
    const c = trashEntry(dive, '200', at(11), 'dives/d1.json', '{}');
    check('bin: an entry keeps the tree fields, not the in-memory payload', !('payload' in c.item) && c.item.name === 'Look', JSON.stringify(c.item));
    check('bin: an entry file is named by batch then id', trashFileName('100', 'p1') === '100__p1.json', trashFileName('100', 'p1'));
    const batches = batchesOf([b, a, c]);
    check('bin: the newest batch comes first', batches.map(x => x.batch).join() === '200,100', JSON.stringify(batches.map(x => x.batch)));
    check(
        'bin: a deleted folder heads its batch and what was in it does not',
        batches[1]?.heads.map(h => h.id).join() === 'f1' && batches[1]?.entries.length === 2,
        JSON.stringify(batches[1]),
    );
    check('bin: a batch says what it holds', describeBatch(batches[1]) === 'folder "Sales" and 1 nested item', describeBatch(batches[1]));
    check('bin: a single item is named on its own', describeBatch(batches[0]) === 'dive "Look"', describeBatch(batches[0]));
    const project = { id: 'root', name: 'Project', type: 'project', parentId: null };
    const back = restoredRepo([project], [a, b]);
    check(
        'bin: a restored folder goes back where it was, with its contents inside it',
        back.find(i => i.id === 'f1')?.parentId === 'root' && back.find(i => i.id === 'p1')?.parentId === 'f1',
        JSON.stringify(back),
    );
    const orphan = trashEntry({ id: 'p2', name: 'X', type: 'pipeline', parentId: 'gone' }, '300', at(12), 'pipelines/p2.json', '{}');
    check(
        'bin: an item whose folder is gone goes back under the project',
        restoredRepo([project], [orphan]).find(i => i.id === 'p2')?.parentId === 'root',
        JSON.stringify(restoredRepo([project], [orphan])),
    );
    check('bin: restoring what is already there adds nothing', restoredRepo(back, [a, b]).length === back.length, JSON.stringify(restoredRepo(back, [a, b])));
}

// ---------------------------------------------------------------------------
// A Sub-flow node's ports are the ports its pipeline declares, by name.
// ---------------------------------------------------------------------------
{
    const sub = {
        nodes: [
            { data: { componentId: 'ctl.subflow.in', properties: { port: 'orders' } } },
            { data: { componentId: 'ctl.subflow.in', properties: {} } },
            { data: { componentId: 'xf.filter', properties: { predicate: 'x > 1' } } },
            { data: { componentId: 'ctl.subflow.out', properties: { port: ' big ' } } },
        ],
    };
    const ports = subflowPorts(sub);
    check(
        'subflow: an input or output is named by its port, "main" when unset, trimmed',
        JSON.stringify(ports) === JSON.stringify({ inputs: ['orders', 'main'], outputs: ['big'] }),
        JSON.stringify(ports),
    );
    check('subflow: a pipeline not loaded offers no ports', subflowPorts(undefined) === null, String(subflowPorts(undefined)));
}

// ---------------------------------------------------------------------------
// Save as sub-flow: the selection moves out, and one Sub-flow node, wired to
// named ports, takes its place.
// ---------------------------------------------------------------------------
{
    const n = (id: string, componentId: string, x: number) => ({ id, type: 'transform', position: { x, y: 0 }, data: { label: id, componentId, properties: {} } });
    const e = (id: string, source: string, target: string, targetHandle = 'main', kind = 'main') => ({
        id, source, target, sourceHandle: 'main', targetHandle, data: { connectionType: kind },
    });
    const pipeline = {
        nodes: [n('a', 'src.csv', 0), n('x', 'src.csv', 0), n('b', 'xf.join.inner', 300), n('c', 'xf.addcol', 600), n('d', 'snk.csv', 900), n('f', 'snk.csv', 900)],
        edges: [e('e1', 'a', 'b'), e('e2', 'x', 'b', 'lookup', 'lookup'), e('e3', 'b', 'c'), e('e4', 'c', 'd'), e('e5', 'c', 'f')],
    };
    const got = extractSubflow(pipeline, ['b', 'c'], 'p_sub');
    const sub = got?.subflow;
    const caller = got?.caller;
    const ports = (cid: string) => (sub?.nodes ?? []).filter(x => x.data.componentId === cid).map(x => x.data.properties?.port);
    check('subflow-extract: one input per outside upstream, "main" first', JSON.stringify(ports('ctl.subflow.in')) === '["main","in_2"]', JSON.stringify(ports('ctl.subflow.in')));
    check('subflow-extract: one output for a step feeding the outside, however many links', JSON.stringify(ports('ctl.subflow.out')) === '["main"]', JSON.stringify(ports('ctl.subflow.out')));
    const lookupIn = (sub?.edges ?? []).find(x => x.target === 'b' && x.targetHandle === 'lookup');
    check('subflow-extract: an input keeps the port it fed inside, a lookup stays a lookup', lookupIn?.data?.connectionType === 'lookup', JSON.stringify(lookupIn));
    check('subflow-extract: the selected steps and their own link move', ['b', 'c'].every(id => sub?.nodes.some(x => x.id === id)) && (sub?.edges ?? []).some(x => x.id === 'e3'), JSON.stringify(sub?.edges));
    const call = caller?.nodes.find(x => x.data.componentId === 'ctl.subflow');
    check('subflow-extract: the selection leaves the caller and a Sub-flow node names the new pipeline', !!call && call.data.properties?.subflowRef === 'p_sub' && !caller?.nodes.some(x => x.id === 'b' || x.id === 'c'), JSON.stringify(caller?.nodes.map(x => x.id)));
    const into = (caller?.edges ?? []).filter(x => x.target === call?.id).map(x => `${x.source}>${x.targetHandle}`).sort();
    check('subflow-extract: each outside upstream is wired to its port', JSON.stringify(into) === '["a>main","x>in_2"]', JSON.stringify(into));
    const from = (caller?.edges ?? []).filter(x => x.source === call?.id).map(x => `${x.sourceHandle}>${x.target}`).sort();
    check('subflow-extract: every outside step the selection fed is fed from the output', JSON.stringify(from) === '["main>d","main>f"]', JSON.stringify(from));
    check('subflow-extract: nothing selected changes nothing', extractSubflow(pipeline, [], 'p') === null, 'not null');
}

// ---------------------------------------------------------------------------
// A run reads a sub-flow from its file, as it reads a Run Job's child: the
// picker stores a pipeline id, and the engine reads a path.
// ---------------------------------------------------------------------------
{
    const repo = [{ id: 'p_sub', name: 'Clean orders', type: 'pipeline', parentId: 'pipelines' } as RepoItem];
    const ref = resolveForRun([node('c', 'ctl.subflow', { subflowRef: 'p_sub' })], repo, 'C:/ws')[0].data.properties?.subflowRef;
    check("subflow: a run turns the sub-flow's id into its file, as for Run Job", ref === 'C:/ws/pipelines/p_sub.json', String(ref));
}

// ---------------------------------------------------------------------------
// A sub-flow may read its own rows and take none in, so a Sub-flow node with
// nothing wired into it is not flagged: the engine says which port is unwired.
// ---------------------------------------------------------------------------
{
    const codes = validatePipeline([node('c', 'ctl.subflow', { subflowRef: 'p_sub' })], []).issues.map(i => i.code);
    check('subflow: a Sub-flow node with no upstream is not flagged as missing one', !codes.includes('missing-required-input'), codes.join(','));
}

// ---------------------------------------------------------------------------
// A run names a sub-flow's steps <node>__<step>; the run panel shows them as
// the engine labels them, "<node> / <step>", nested sub-flows included.
// ---------------------------------------------------------------------------
{
    const step = (id: string, componentId: string, label: string, properties: Record<string, unknown> = {}) => ({
        id, data: { label, componentId, properties },
    });
    const pipelines: Record<string, { nodes: ReturnType<typeof step>[] }> = {
        p_outer: { nodes: [step('in', 'ctl.subflow.in', 'In'), step('tidy', 'xf.filter', 'Tidy'),
                           step('inner', 'ctl.subflow', 'Inner', { subflowRef: 'p_inner' }), step('out', 'ctl.subflow.out', 'Out')] },
        p_inner: { nodes: [step('in', 'ctl.subflow.in', 'In'), step('dbl', 'xf.addcol', 'Double'), step('out', 'ctl.subflow.out', 'Out')] },
    };
    const labels = subflowStepLabels([step('call', 'ctl.subflow', 'Clean', { subflowRef: 'p_outer' })], id => pipelines[id]);
    check(
        'subflow-labels: steps, a nested call and its steps are named as the engine names them; boundaries are not steps',
        JSON.stringify(labels) === JSON.stringify({
            call__tidy: 'Clean / Tidy', call__inner: 'Clean / Inner', call__inner__dbl: 'Clean / Inner / Double',
        }),
        JSON.stringify(labels),
    );
}

if (failures.length) {
    console.error(`\ncheck-logic: ${failures.length} check(s) failed:\n`);
    for (const f of failures) console.error(`  - ${f}`);
    process.exit(1);
}
console.log('check-logic: all checks passed');
