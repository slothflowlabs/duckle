import { useContext, useEffect, useRef, useState } from 'react';
import { Handle, Position, type Node, type NodeProps } from '@xyflow/react';
import { ChevronDown, ChevronRight } from 'lucide-react';
import type { PipelineGroup } from '../groups';
import { FrameContext } from '../frame-context';
import { RunStatusContext } from '../run-status-context';

export type FrameData = { group: PipelineGroup; steps: number };

/**
 * A group's frame. Expanded, it is drawn behind its steps and only its header
 * takes the pointer, so the steps inside stay clickable and a box selection can
 * start inside it. Collapsed, it is a card that stands in for the steps, and the
 * links that crossed into the group are drawn to it.
 */
export default function GroupFrame({ id, data, selected }: NodeProps<Node<FrameData>>) {
    const { editing, setEditing, rename, toggle } = useContext(FrameContext);
    const runs = useContext(RunStatusContext);
    const group = data.group;
    const isEditing = editing === id;
    const [draft, setDraft] = useState(group.label);
    const input = useRef<HTMLInputElement>(null);
    // Escape unmounts the field, and the blur that may follow must not save it.
    const cancelled = useRef(false);

    useEffect(() => {
        if (!isEditing) return;
        cancelled.current = false;
        setDraft(group.label);
        const frame = requestAnimationFrame(() => {
            input.current?.focus();
            input.current?.select();
        });
        return () => cancelAnimationFrame(frame);
        // Only on entering the field: a re-render while typing must not reset it.
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [isEditing]);

    const finish = () => {
        const label = draft.trim();
        if (!cancelled.current && label && label !== group.label) rename(id, label);
        setEditing(null);
    };

    // How the group's steps did in the last run, for a collapsed card that hides them.
    const states = group.members.map(m => runs[m]?.status).filter(Boolean);
    const status = states.includes('error')
        ? 'error'
        : states.includes('running')
          ? 'running'
          : states.length > 0
            ? 'ok'
            : null;

    return (
        <div
            className={'canvas-frame' + (group.collapsed ? ' is-collapsed' : '') + (selected ? ' is-selected' : '')}
            data-color={group.color ?? 'orange'}
        >
            <div className="canvas-frame-head">
                <button
                    type="button"
                    className="canvas-frame-toggle nodrag"
                    onClick={() => toggle(id)}
                    title={group.collapsed ? 'Expand group' : 'Collapse group'}
                    aria-label={group.collapsed ? 'Expand group' : 'Collapse group'}
                >
                    {group.collapsed ? <ChevronRight size={14} /> : <ChevronDown size={14} />}
                </button>
                {isEditing ? (
                    <input
                        ref={input}
                        className="canvas-frame-input nodrag"
                        value={draft}
                        onChange={e => setDraft(e.target.value)}
                        onBlur={finish}
                        onKeyDown={e => {
                            if (e.key === 'Enter') {
                                e.preventDefault();
                                input.current?.blur();
                            } else if (e.key === 'Escape') {
                                e.preventDefault();
                                cancelled.current = true;
                                setEditing(null);
                            }
                        }}
                    />
                ) : (
                    <span className="canvas-frame-label" onDoubleClick={() => setEditing(id)} title="Double-click to rename">
                        {group.label}
                    </span>
                )}
                <span className="canvas-frame-count">
                    {data.steps} step{data.steps === 1 ? '' : 's'}
                </span>
                {status ? (
                    <span
                        className={'canvas-frame-status is-' + status}
                        title={status === 'error' ? 'A step in this group failed' : status === 'running' ? 'Running' : 'Every step in this group ran'}
                    />
                ) : null}
            </div>
            {group.collapsed ? (
                <>
                    <Handle type="target" position={Position.Left} id="group-in" isConnectable={false} />
                    <Handle type="source" position={Position.Right} id="group-out" isConnectable={false} />
                </>
            ) : null}
        </div>
    );
}
