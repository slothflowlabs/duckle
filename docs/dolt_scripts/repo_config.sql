  select
    'rates' as repo_key,
    'post-no-preference/rates' as remote_url,
    'master' as branch,
    '.stitchly/cache/dolt' as cache_root,
    '.stitchly/state/dolt_sync.duckdb' as state_db,
    'artifacts/dolt' as artifact_root,
    false as force_snapshot;