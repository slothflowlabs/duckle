import type { Schedule, ScheduleKind } from './tauri-bridge';

/** The fields the desktop Schedules dialog edits. */
export type ScheduleEdit = {
    id: string;
    pipelineId: string;
    name: string;
    enabled: boolean;
    kind: ScheduleKind;
    /**
     * #317: set only when the dialog showed the pipeline's declared parameters.
     * Left out, the values the schedule already has are kept.
     */
    params?: Record<string, string>;
};

/**
 * The schedule as the server's deploy endpoint wants it.
 *
 * The desktop keeps a schedule as `kind: { type: 'cron' | 'interval' | 'file_watch' }`;
 * `save_schedule_at` on the server reads flat `cron` / `intervalSeconds` keys instead.
 * Sending the desktop shape would deploy a schedule the server quietly ignores, so it is
 * translated here rather than hoped over.
 *
 * A file watch has no expression that endpoint can store, so it is not sent at all.
 *
 * The zone and exclusion calendar go too. Sending only the trigger put a Brussels
 * 03:00 job on the server's clock, usually UTC, with its maintenance days
 * forgotten. A schedule without them sends neither key, which the server reads as
 * "leave what is there alone".
 */
export function serverSchedule(s: Schedule | undefined, name: string): Record<string, unknown> | null {
    if (!s) return null;
    const trigger =
        s.kind.type === 'cron'
            ? { cron: s.kind.expr }
            : s.kind.type === 'interval'
              ? { intervalSeconds: s.kind.seconds }
              : null;
    if (!trigger) return null;
    return {
        id: name,
        enabled: false,
        ...trigger,
        ...(s.timezone ? { timezone: s.timezone } : {}),
        ...(s.exclude ? { exclude: s.exclude } : {}),
        ...(s.params ? { params: s.params } : {}),
        // #296: the server's schedule API takes the policies now; absent keeps its own.
        ...(s.overlap ? { overlap: s.overlap } : {}),
        ...(s.misfire ? { misfire: s.misfire } : {}),
        ...(s.catchup ? { catchup: s.catchup } : {}),
    };
}

/**
 * Run a Schedules-dialog action; the message to show when it fails, else null.
 *
 * "Run now" and "Delete" awaited their command inside try/finally with no catch,
 * so a failure became an unhandled rejection and the dialog showed nothing -
 * including "already running in this workspace, so this run was refused", which
 * is exactly what the person pressing Run now needs to read.
 */
export async function scheduleActionError(action: () => Promise<unknown>): Promise<string | null> {
    try {
        await action();
        return null;
    } catch (err) {
        return String(err);
    }
}

/**
 * The record the desktop Schedules dialog saves: the schedule as it was loaded,
 * with only the fields the dialog edits replaced.
 *
 * The store holds more than the dialog shows - a timezone, an exclusion
 * calendar, a misfire policy and catch-up bounds, set through the server API -
 * and the desktop upsert replaces the whole record. Building a fresh object from
 * the dialog's own fields sent none of them, so renaming a schedule put a
 * Brussels 03:00 job back on the machine's clock and switched off its
 * maintenance calendar. The fix belongs here rather than in the upsert: there,
 * "not sent" and "cleared" arrive as the same default, and a merge would make an
 * exclusion calendar impossible to clear.
 */
export function scheduleForSave(loaded: Schedule | undefined, edit: ScheduleEdit): Schedule {
    return {
        ...loaded,
        id: edit.id,
        pipeline_id: edit.pipelineId,
        name: edit.name.trim() || 'Schedule',
        enabled: edit.enabled,
        kind: edit.kind,
        ...(edit.params !== undefined ? { params: edit.params } : {}),
    };
}
