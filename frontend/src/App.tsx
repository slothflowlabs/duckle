import { useEffect, useState } from 'react';
import { invoke } from '@tauri-apps/api/core';
import EditorTabs from './workflow-ui/EditorTabs';
import EngineSelector, { type EngineId } from './workflow-ui/EngineSelector';
import Palette from './workflow-ui/Palette';

type RuntimeState = 'connecting' | 'ready' | 'offline';

export default function App() {
    const [runtime, setRuntime] = useState<RuntimeState>('connecting');
    const [engine, setEngine] = useState<EngineId>('duckdb');

    useEffect(() => {
        let cancelled = false;
        invoke<string>('ping')
            .then(reply => {
                if (!cancelled) setRuntime(reply === 'pong' ? 'ready' : 'offline');
            })
            .catch(() => {
                if (!cancelled) setRuntime('offline');
            });
        return () => {
            cancelled = true;
        };
    }, []);

    return (
        <div className="app">
            <header className="topbar">
                <div className="brand">
                    <span className="brand-mark">◇</span> Duckle
                </div>
                <div className="topbar-sep" aria-hidden="true" />
                <EngineSelector value={engine} onChange={setEngine} />
                <div className="topbar-spacer" />
                <div className="status" data-state={runtime}>
                    <span className="status-dot" /> runtime: {runtime}
                </div>
            </header>

            <main className="workspace">
                <Palette />
                <section className="canvas-shell">
                    <EditorTabs engine={engine} />
                </section>
            </main>
        </div>
    );
}
