import { createContext } from 'react';

/** What a group's frame needs from the canvas: which frame is being renamed, and how to change one. */
export type FrameActions = {
    editing: string | null;
    setEditing: (id: string | null) => void;
    rename: (id: string, label: string) => void;
    toggle: (id: string) => void;
};

export const FrameContext = createContext<FrameActions>({
    editing: null,
    setEditing: () => {},
    rename: () => {},
    toggle: () => {},
});
