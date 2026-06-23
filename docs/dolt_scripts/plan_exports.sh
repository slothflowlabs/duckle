 set -eu

  workspace="${DUCKLE_WORKSPACE:-$PWD}"
  duckdb_bin="${DUCKLE_DUCKDB_BIN:-duckdb}"
  artifact_root="${DOLT_ARTIFACT_ROOT:-artifacts/dolt}"

  csv_value() {
    tail -n +2 | tr -d '\r' | sed 's/^"//; s/"$//'
  }

  json_escape() {
    sed 's/\\/\\\\/g; s/"/\\"/g'
  }

  safe_path_part() {
    sed 's#[^A-Za-z0-9._-]#_#g'
  }

  input_value() {
    col="$1"
    "$duckdb_bin" -csv "$DUCKLE_DUCKDB_DATABASE" -c "select coalesce(cast(\"${col}\" as varchar), '') from \"$DUCKLE_INPUT_TABLE\" limit 1" | csv_value
  }

  repo_key="$(input_value repo_key)"
  branch="$(input_value branch)"
  repo_path="$(input_value repo_path)"
  previous_commit="$(input_value previous_commit)"
  head_commit="$(input_value head_commit)"
  sync_ok="$(input_value sync_ok)"
  should_skip="$(input_value should_skip)"

  if [ "$sync_ok" != "true" ]; then
    echo "plan_exports: upstream sync did not succeed" >&2
    exit 1
  fi

  if [ "$should_skip" = "true" ]; then
    echo "plan_exports: upstream row is marked should_skip=true; route wiring is wrong" >&2
    exit 1
  fi

  cd "$workspace"

  if [ ! -d "$repo_path/.dolt" ]; then
    echo "plan_exports: Dolt repo not found at $repo_path" >&2
    exit 1
  fi

  safe_branch="$(printf '%s' "$branch" | safe_path_part)"

  table_list="$(
    cd "$repo_path" &&
    dolt ls |
      sed '1d; s/^[[:space:]]*//; s/[[:space:]]*$//; /^$/d'
  )"

  if [ -z "$table_list" ]; then
    echo "plan_exports: no base tables found in $repo_path" >&2
    exit 1
  fi

  reason="changed"
  if [ -z "$previous_commit" ]; then
    reason="initial_load"
  fi

  printf '%s\n' "$table_list" | while IFS= read -r table_name; do
    safe_table="$(printf '%s' "$table_name" | safe_path_part)"

    stage_path=".stitchly/tmp/dolt/${repo_key}/${safe_branch}/${head_commit}/${safe_table}/snapshot.parquet"
    snapshot_path="${artifact_root}/${repo_key}/${safe_branch}/${safe_table}/snapshots/commit=${head_commit}/data.parquet"

    repo_key_json="$(printf '%s' "$repo_key" | json_escape)"
    branch_json="$(printf '%s' "$branch" | json_escape)"
    repo_path_json="$(printf '%s' "$repo_path" | json_escape)"
    table_name_json="$(printf '%s' "$table_name" | json_escape)"
    previous_commit_json="$(printf '%s' "$previous_commit" | json_escape)"
    head_commit_json="$(printf '%s' "$head_commit" | json_escape)"
    reason_json="$(printf '%s' "$reason" | json_escape)"
    stage_path_json="$(printf '%s' "$stage_path" | json_escape)"
    snapshot_path_json="$(printf '%s' "$snapshot_path" | json_escape)"

    printf '%s' '{"repo_key":"'
    printf '%s' "$repo_key_json"
    printf '%s' '","branch":"'
    printf '%s' "$branch_json"
    printf '%s' '","repo_path":"'
    printf '%s' "$repo_path_json"
    printf '%s' '","table_name":"'
    printf '%s' "$table_name_json"
    printf '%s' '","previous_commit":"'
    printf '%s' "$previous_commit_json"
    printf '%s' '","head_commit":"'
    printf '%s' "$head_commit_json"
    printf '%s' '","export_mode":"snapshot","reason":"'
    printf '%s' "$reason_json"
    printf '%s' '","stage_path":"'
    printf '%s' "$stage_path_json"
    printf '%s' '","snapshot_path":"'
    printf '%s' "$snapshot_path_json"
    printf '%s\n' '","delta_path":"","should_export":true}'
  done