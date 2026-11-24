// The swarm's transport. The desktop app streams events over a Tauri channel;
// the web editor POSTs to /api/swarm_stream and reads Server-Sent Events, the
// same way a run streams (tauri-bridge runViaSse). Both end with the
// `finished` event.

import { Channel, invoke } from '@tauri-apps/api/core';
import { isWebBackend } from '../web-fs';
import type { SwarmEvent } from './swarm-state';

export type ProviderKind = 'anthropic-account' | 'anthropic-key' | 'openai-compatible';

export type SwarmProvider = {
    kind: ProviderKind;
    model?: string;
    baseUrl?: string;
    /** For this run only: never stored. */
    apiKey?: string;
};

export type SwarmRequest = {
    goal: string;
    /** The open workspace. The web editor's server uses its own. */
    workspace: string;
    provider: SwarmProvider;
};

export type SwarmStatus = {
    anthropicAccount: { installed: boolean; signedIn: boolean };
    keys: Record<string, boolean>;
    defaultModel: string;
};

export async function swarmStatus(): Promise<SwarmStatus | null> {
    try {
        return await invoke<SwarmStatus | null>('swarm_status');
    } catch {
        return null;
    }
}

type Finished = Extract<SwarmEvent, { type: 'finished' }>;

function failed(summary: string): Finished {
    return { type: 'finished', ok: false, summary };
}

/**
 * Run one swarm. `onEvent` sees every event, the `finished` one included,
 * which is also returned. On the web, aborting `signal` stops the swarm.
 */
export async function swarmRun(
    request: SwarmRequest,
    onEvent: (e: SwarmEvent) => void,
    signal?: AbortSignal,
): Promise<Finished> {
    if (isWebBackend()) return runViaSse(request, onEvent, signal);
    const channel = new Channel<SwarmEvent>();
    channel.onmessage = onEvent;
    try {
        return await invoke<Finished>('swarm_run', { request, onEvent: channel });
    } catch (err) {
        const done = failed(String(err));
        onEvent({ type: 'error', message: String(err) });
        onEvent(done);
        return done;
    }
}

/** Stop the desktop's running swarm. The web stops by aborting the request. */
export async function swarmStop(): Promise<void> {
    if (isWebBackend()) return;
    try {
        await invoke('swarm_stop');
    } catch {
        // Nothing running.
    }
}

async function runViaSse(
    request: SwarmRequest,
    onEvent: (e: SwarmEvent) => void,
    signal?: AbortSignal,
): Promise<Finished> {
    const fail = (summary: string): Finished => {
        const done = failed(summary);
        onEvent({ type: 'error', message: summary });
        onEvent(done);
        return done;
    };
    let res: Response;
    try {
        res = await fetch('/api/swarm_stream', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(request),
            signal,
        });
    } catch (err) {
        if (signal?.aborted) return failed('Stopped.');
        return fail(String(err));
    }
    if (!res.ok || !res.body) {
        const text = (await res.text().catch(() => '')).trim();
        let reason = text;
        try {
            const parsed = JSON.parse(text) as { error?: unknown };
            if (typeof parsed.error === 'string') reason = parsed.error;
        } catch {
            // plain text
        }
        return fail(`The swarm could not start: HTTP ${res.status}${reason ? ` - ${reason}` : ''}`);
    }
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';
    let result: Finished | null = null;
    try {
        for (;;) {
            const { done, value } = await reader.read();
            if (done) break;
            buf += decoder.decode(value, { stream: true });
            let sep: number;
            while ((sep = buf.indexOf('\n\n')) >= 0) {
                const frame = buf.slice(0, sep);
                buf = buf.slice(sep + 2);
                let isResult = false;
                let data = '';
                for (const line of frame.split('\n')) {
                    if (line.startsWith('event:')) isResult = line.slice(6).trim() === 'result';
                    else if (line.startsWith('data:')) data += line.slice(5).trim();
                }
                if (!data) continue;
                try {
                    const evt = JSON.parse(data) as SwarmEvent;
                    // The finished event arrives twice: as a data frame, and
                    // again as the result. Show it once.
                    if (isResult) result = evt as Finished;
                    else onEvent(evt);
                } catch {
                    // A malformed frame.
                }
            }
        }
    } catch (err) {
        if (signal?.aborted) return failed('Stopped.');
        return fail(String(err));
    }
    return result ?? fail('The swarm ended without finishing.');
}
