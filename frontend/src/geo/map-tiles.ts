// #372: the tile server a map draws its shapes over. None unless the user names
// one in Settings: a tile server sees the area being looked at, so a preview
// reaches the network only once the user has said it may.

import { loadPersisted, savePersisted } from '../persistence';

/** An XYZ tile template, and the credit its terms ask to be shown on the map. */
export type MapTiles = { url: string; attribution: string };

const KEY = 'mapTiles';

export function readMapTiles(): MapTiles {
    const v = loadPersisted<Partial<MapTiles> | null>(KEY, null);
    return {
        url: typeof v?.url === 'string' ? v.url : '',
        attribution: typeof v?.attribution === 'string' ? v.attribution : '',
    };
}

export function saveMapTiles(tiles: MapTiles): void {
    savePersisted(KEY, tiles);
}
