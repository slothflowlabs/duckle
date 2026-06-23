set -eu

  workspace="${DUCKLE_WORKSPACE:-$PWD}"
  duckdb_bin="${DUCKLE_DUCKDB_BIN:-duckdb}"
  state_db=".stitchly/state/dolt_sync.duckdb"

  json_escape() {
    sed 's/\\/\\\\/g; s/"/\\"/g'
  }

  sql_escape() {
    sed "s/'/''/g"
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
    echo "publish_and_update_state: upstream input file is empty or missing" >&2
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
  row_count="$(json_field row_count "$DUCKLE_INPUT_PATH")"
  file_size_bytes="$(json_field file_size_bytes "$DUCKLE_INPUT_PATH")"
  export_validation_ok="$(json_field export_validation_ok "$DUCKLE_INPUT_PATH")"
  export_validation_status="$(json_field export_validation_status "$DUCKLE_INPUT_PATH")"

  if [ "$export_validation_ok" != "true" ]; then
    echo "publish_and_update_state: validation failed: $export_validation_status" >&2
    exit 1
  fi

  if [ ! -s "$stage_path" ]; then
    echo "publish_and_update_state: staged file missing or empty: $stage_path" >&2
    exit 1
  fi

  mkdir -p "$(dirname "$snapshot_path")"
  mkdir -p "$(dirname "$state_db")"

  tmp_publish="${snapshot_path}.tmp.$$"
  rm -f "$tmp_publish"

  cp "$stage_path" "$tmp_publish"

  if [ ! -s "$tmp_publish" ]; then
    echo "publish_and_update_state: temp publish file missing or empty: $tmp_publish" >&2
    exit 1
  fi

  mv "$tmp_publish" "$snapshot_path"

  published_size="$(wc -c < "$snapshot_path" | tr -d ' ')"

  if [ "$published_size" != "$file_size_bytes" ]; then
    echo "publish_and_update_state: published size mismatch staged=$file_size_bytes published=$published_size" >&2
    exit 1
  fi

  repo_key_sql="$(printf '%s' "$repo_key" | sql_escape)"
  branch_sql="$(printf '%s' "$branch" | sql_escape)"
  table_name_sql="$(printf '%s' "$table_name" | sql_escape)"
  remote_url_sql="$(printf '%s' "" | sql_escape)"
  head_commit_sql="$(printf '%s' "$head_commit" | sql_escape)"
  snapshot_path_sql="$(printf '%s' "$snapshot_path" | sql_escape)"

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

  insert into dolt_sync (
    repo_key,
    remote_url,
    branch,
    table_name,
    last_processed_commit,
    last_snapshot_commit,
    schema_hash,
    artifact_manifest_path,
    row_count,
    updated_at
  )
  values (
    '${repo_key_sql}',
    '${remote_url_sql}',
    '${branch_sql}',
    '${table_name_sql}',
    '${head_commit_sql}',
    '${head_commit_sql}',
    '',
    '${snapshot_path_sql}',
    ${row_count},
    current_timestamp
  );
  " >&2

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
  printf '%s' "$file_size_bytes"
  printf '%s' ',"published_size_bytes":'
  printf '%s' "$published_size"
  printf '%s\n' ',"publish_ok":true,"state_updated":true}'