// A dashboard is a saved collection of dives shown on one page. It references
// dives by id (the dives stay independent + live-querying). See docs/design/dives.md.

export const DASHBOARD_SCHEMA_VERSION = 1;

export interface Dashboard {
    dashboardSchemaVersion: number;
    id: string;
    title: string;
    description?: string;
    diveIds: string[];
}

export interface DashboardParseResult {
    ok: boolean;
    dashboard?: Dashboard;
    error?: string;
}

export function parseDashboard(raw: unknown): DashboardParseResult {
    if (typeof raw !== 'object' || raw === null) {
        return { ok: false, error: 'Dashboard is not a JSON object.' };
    }
    const d = raw as Record<string, unknown>;
    const ver = d.dashboardSchemaVersion;
    if (typeof ver !== 'number' || Math.floor(ver) > DASHBOARD_SCHEMA_VERSION) {
        return { ok: false, error: `Unsupported dashboard schema version: ${String(ver)}.` };
    }
    if (typeof d.id !== 'string' || !d.id) return { ok: false, error: 'Dashboard is missing "id".' };
    if (typeof d.title !== 'string' || !d.title) return { ok: false, error: 'Dashboard is missing "title".' };
    if (!Array.isArray(d.diveIds) || !d.diveIds.every((x) => typeof x === 'string')) {
        return { ok: false, error: 'Dashboard "diveIds" must be an array of dive ids.' };
    }
    return { ok: true, dashboard: raw as Dashboard };
}
