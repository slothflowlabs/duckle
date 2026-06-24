// Create / edit / view a dashboard: a grid of dives, each live-querying via its
// own DivePanel. Edit mode picks which saved dives to include. See docs/design/dives.md.

import { useState } from 'react';
import type { RepoItem } from '../repo-types';
import { DivePanel } from './DivePanel';
import { loadDive } from './dive-io';
import { DASHBOARD_SCHEMA_VERSION, parseDashboard, type Dashboard } from './dashboard-types';
import type { Dive } from './dive-types';

interface DashboardModalProps {
    item: RepoItem | null;
    diveItems: RepoItem[];
    workspacePath: string | null;
    theme?: 'light' | 'dark';
    onClose: () => void;
    onSave: (name: string, dashboard: Dashboard) => void;
}

function newId(): string {
    return 'dash_' + Date.now().toString(36) + '_' + Math.random().toString(36).slice(2, 6);
}

export function DashboardModal({
    item,
    diveItems,
    workspacePath,
    theme,
    onClose,
    onSave,
}: DashboardModalProps) {
    let existing: Dashboard | null = null;
    let loadError: string | null = null;
    if (item?.payload) {
        const r = parseDashboard(item.payload);
        if (r.ok && r.dashboard) existing = r.dashboard;
        else loadError = r.error ?? 'Invalid dashboard.';
    }

    const isCreate = !existing && !loadError;
    const [editing, setEditing] = useState(isCreate);
    const [title, setTitle] = useState(existing?.title ?? item?.name ?? 'New dashboard');
    const [selected, setSelected] = useState<string[]>(existing?.diveIds ?? []);

    const options = diveItems.map((d) => {
        let dive: Dive | null = null;
        try {
            if (d.payload) dive = loadDive(d.payload);
        } catch {
            dive = null;
        }
        return { id: d.id, name: d.name, dive };
    });
    const byId = new Map(options.map((o) => [o.id, o]));

    const toggle = (id: string) =>
        setSelected((s) => (s.includes(id) ? s.filter((x) => x !== id) : [...s, id]));
    const save = () =>
        onSave(title.trim() || 'Untitled dashboard', {
            dashboardSchemaVersion: DASHBOARD_SCHEMA_VERSION,
            id: existing?.id ?? newId(),
            title: title.trim() || 'Untitled dashboard',
            diveIds: selected,
        });

    return (
        <div className="dive-modal-backdrop" onClick={onClose}>
            <div className="dive-modal dash-modal" onClick={(e) => e.stopPropagation()}>
                <div className="dive-modal-head">
                    <span>{editing ? (isCreate ? 'New dashboard' : 'Edit dashboard') : item?.name ?? 'Dashboard'}</span>
                    <div className="dive-modal-actions">
                        {!editing ? (
                            <button className="dive-btn" onClick={() => setEditing(true)}>
                                Edit
                            </button>
                        ) : null}
                        <button className="dive-modal-x" onClick={onClose} aria-label="Close" title="Close">
                            ×
                        </button>
                    </div>
                </div>
                <div className="dive-modal-body">
                    {loadError ? (
                        <div className="dive-panel-msg dive-panel-err">{loadError}</div>
                    ) : editing ? (
                        <div className="dive-editor">
                            <label className="dive-field">
                                <span>Title</span>
                                <input value={title} onChange={(e) => setTitle(e.target.value)} />
                            </label>
                            <div className="dive-field">
                                <span>Dives in this dashboard</span>
                                <div className="dash-picker">
                                    {options.length === 0 ? (
                                        <div className="dive-panel-msg">No dives yet - create some dives first.</div>
                                    ) : (
                                        options.map((o) => (
                                            <label key={o.id} className="dash-pick-row">
                                                <input
                                                    type="checkbox"
                                                    checked={selected.includes(o.id)}
                                                    onChange={() => toggle(o.id)}
                                                />
                                                <span>{o.name}</span>
                                            </label>
                                        ))
                                    )}
                                </div>
                            </div>
                            <div className="dive-editor-actions">
                                <button
                                    className="dive-btn primary"
                                    onClick={save}
                                    disabled={!title.trim() || selected.length === 0}
                                >
                                    Save
                                </button>
                            </div>
                        </div>
                    ) : (
                        <div className="dash-grid">
                            {selected.map((id) => {
                                const o = byId.get(id);
                                if (!o?.dive) {
                                    return (
                                        <div key={id} className="dash-cell dive-panel-msg">
                                            Missing dive: {id}
                                        </div>
                                    );
                                }
                                return (
                                    <div key={id} className="dash-cell">
                                        <DivePanel dive={o.dive} workspacePath={workspacePath} theme={theme} />
                                    </div>
                                );
                            })}
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
}
