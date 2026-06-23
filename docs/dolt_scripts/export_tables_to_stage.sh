set -eu

  workspace="${DUCKLE_WORKSPACE:-$PWD}"
  duckdb_bin="${DUCKLE_DUCKDB_BIN:-duckdb}"

  json_escape() {
    sed 's/\\/\\\\/g; s/"/\\"/g'
  }

  json_field() {
    field="$1"
    file="$2"
    "$duckdb_bin" -csv -c "select coalesce(cast(${field} as varchar), '') from read_json_auto('${file}') limit 1" |
      tail -n +2 |
      tr -d '\r' |
      sed 's/^"//; s/"$//'
  }

  cd "$workspace"

  if [ ! -s "$DUCKLE_INPUT_PATH" ]; then
    echo "export_tables_to_stage: upstream input file is empty or missing" >&2
    exit 1
  fi

  repo_key="$(json_field repo_key "$DUCKLE_INPUT_PATH")"
  branch="$(json_field branch "$DUCKLE_INPUT_PATH")"
  repo_path="$(json_field repo_path "$DUCKLE_INPUT_PATH")"
  table_name="$(json_field table_name "$DUCKLE_INPUT_PATH")"
  previous_commit="$(json_field previous_commit "$DUCKLE_INPUT_PATH")"
  head_commit="$(json_field head_commit "$DUCKLE_INPUT_PATH")"
  export_mode="$(json_field export_mode "$DUCKLE_INPUT_PATH")"
  reason="$(json_field reason "$DUCKLE_INPUT_PATH")"
  stage_path="$(json_field stage_path "$DUCKLE_INPUT_PATH")"
  snapshot_path="$(json_field snapshot_path "$DUCKLE_INPUT_PATH")"
  plan_ok="$(json_field plan_ok "$DUCKLE_INPUT_PATH")"
  should_export="$(json_field should_export "$DUCKLE_INPUT_PATH")"

  if [ "$plan_ok" != "true" ]; then
    echo "export_tables_to_stage: plan_ok is not true" >&2
    exit 1
  fi

  if [ "$should_export" != "true" ]; then
    echo "export_tables_to_stage: should_export is not true" >&2
    exit 1
  fi

  if [ "$export_mode" != "snapshot" ]; then
    echo "export_tables_to_stage: unsupported export_mode=$export_mode" >&2
    exit 1
  fi

  if [ ! -d "$repo_path/.dolt" ]; then
    echo "export_tables_to_stage: Dolt repo not found at $repo_path" >&2
    exit 1
  fi

  mkdir -p "$(dirname "$stage_path")"

  row_count="$(
    cd "$repo_path" &&
    dolt sql -r csv -q "select count(*) as n from $table_name" |
      tail -n +2 |
      tr -d '\r' |
      sed 's/^"//; s/"$//'
  )"

  rm -f "$stage_path"

  (
    cd "$repo_path"
    dolt table export --force --file-type parquet "$table_name" "$workspace/$stage_path" >&2
  )

  if [ ! -s "$stage_path" ]; then
    echo "export_tables_to_stage: export did not create non-empty file at $stage_path" >&2
    exit 1
  fi

  file_size="$(wc -c < "$stage_path" | tr -d ' ')"

  repo_key_json="$(printf '%s' "$repo_key" | json_escape)"
  branch_json="$(printf '%s' "$branch" | json_escape)"
  repo_path_json="$(printf '%s' "$repo_path" | json_escape)"
  table_name_json="$(printf '%s' "$table_name" | json_escape)"
  previous_commit_json="$(printf '%s' "$previous_commit" | json_escape)"
  head_commit_json="$(printf '%s' "$head_commit" | json_escape)"
  export_mode_json="$(printf '%s' "$export_mode" | json_escape)"
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
  printf '%s' '","export_mode":"'
  printf '%s' "$export_mode_json"
  printf '%s' '","reason":"'
  printf '%s' "$reason_json"
  printf '%s' '","stage_path":"'
  printf '%s' "$stage_path_json"
  printf '%s' '","snapshot_path":"'
  printf '%s' "$snapshot_path_json"
  printf '%s' '","row_count":'
  printf '%s' "$row_count"
  printf '%s' ',"file_size_bytes":'
  printf '%s' "$file_size"
  printf '%s\n' ',"export_ok":true}'