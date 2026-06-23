set -eu

  workspace="${DUCKLE_WORKSPACE:-$PWD}"
  duckdb_bin="${DUCKLE_DUCKDB_BIN:-duckdb}"

  csv_value() {
    tail -n +2 | tr -d '\r' | sed 's/^"//; s/"$//'
  }

  sql_escape() {
    sed "s/'/''/g"
  }

  json_escape() {
    sed 's/\\/\\\\/g; s/"/\\"/g'
  }

  config_value() {
    _out="$("$duckdb_bin" -csv "$DUCKLE_DUCKDB_DATABASE" -c \
      "select $1 from \"$DUCKLE_INPUT_TABLE\" limit 1")"
    printf '%s\n' "$_out" | csv_value
  }

  repo_key="$(config_value repo_key)"
  remote_url="$(config_value remote_url)"
  branch="$(config_value branch)"
  cache_root="$(config_value cache_root)"
  state_db="$(config_value state_db)"

  cd "$workspace"

  repo_dir="${cache_root}/${repo_key}/repo"
  mkdir -p "$(dirname "$repo_dir")" "$(dirname "$state_db")"

  if [ ! -d "$repo_dir/.dolt" ]; then
    dolt clone "$remote_url" "$repo_dir" >&2
  fi

  cd "$repo_dir"

  if ! dolt config --local --get user.name >/dev/null 2>&1; then
    dolt config --local --add user.name "Stitchly Sync"
  fi

  if ! dolt config --local --get user.email >/dev/null 2>&1; then
    dolt config --local --add user.email "stitchly-sync@example.local"
  fi

  dolt checkout "$branch" >&2
  dolt pull >&2

  head_csv="$(dolt sql -r csv -q "select commit_hash from dolt_log order by commit_order desc limit 1")"
  head_commit="$(printf '%s\n' "$head_csv" | csv_value)"

  if [ -z "$head_commit" ]; then
    echo "sync_repo: could not resolve head commit" >&2
    exit 1
  fi

  cd "$workspace"

  "$duckdb_bin" "$state_db" -c "
  create table if not exists dolt_sync (
    repo_key text,
    remote_url text,
    branch text,
    table_name text,
    last_processed_commit text,
    last_snapshot_commit text,
    schema_hash text,
    artifact_manifest_path text,
    row_count bigint,
    updated_at timestamp
  );
  " >&2

  repo_key_sql="$(printf '%s' "$repo_key" | sql_escape)"
  branch_sql="$(printf '%s' "$branch" | sql_escape)"

  previous_csv="$("$duckdb_bin" -csv "$state_db" -c "
  select coalesce((
    select last_processed_commit
    from dolt_sync
    where repo_key = '${repo_key_sql}'
      and branch = '${branch_sql}'
    order by updated_at desc
    limit 1
  ), '');
  ")"
  previous_commit="$(printf '%s\n' "$previous_csv" | csv_value)"

  should_skip="false"
  if [ -n "$previous_commit" ] && [ "$previous_commit" = "$head_commit" ]; then
    should_skip="true"
  fi

  repo_key_json="$(printf '%s' "$repo_key" | json_escape)"
  remote_url_json="$(printf '%s' "$remote_url" | json_escape)"
  branch_json="$(printf '%s' "$branch" | json_escape)"
  repo_dir_json="$(printf '%s' "$repo_dir" | json_escape)"
  previous_commit_json="$(printf '%s' "$previous_commit" | json_escape)"
  head_commit_json="$(printf '%s' "$head_commit" | json_escape)"

  printf '{"repo_key":"%s","remote_url":"%s","branch":"%s","repo_path":"%s","previous_commit":"%s","head_commit":"%s","should_skip":%s}\n' \
    "$repo_key_json" \
    "$remote_url_json" \
    "$branch_json" \
    "$repo_dir_json" \
    "$previous_commit_json" \
    "$head_commit_json" \
    "$should_skip"