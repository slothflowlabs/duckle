/**
 * Which screen the desktop opens on, from DuckDB's engine status: the engine
 * setup, or the app.
 *
 * Only the DuckDB this build pins opens the app. An outdated one blocks like a
 * missing one, until it is upgraded: it used to run on behind a banner the user
 * could dismiss, so an install kept the DuckDB it first downloaded, 1.5.4 under
 * a Duckle built for 2.0 (#410). A DuckDB the user chose through
 * DUCKLE_DUCKDB_BIN reads installed and never outdated (#373), so it opens.
 */
export function engineGateFor(
    duck: { installed: boolean; outdated: boolean } | undefined,
): 'engine-setup' | 'ready' {
    return duck?.installed ? 'ready' : 'engine-setup';
}
