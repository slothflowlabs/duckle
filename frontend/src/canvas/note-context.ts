import { createContext } from 'react';

/** What a canvas note needs from the canvas: which note is being written in, and how to save it. */
export type NoteActions = {
    editing: string | null;
    setEditing: (id: string | null) => void;
    commit: (id: string, text: string) => void;
};

export const NoteContext = createContext<NoteActions>({
    editing: null,
    setEditing: () => {},
    commit: () => {},
});
