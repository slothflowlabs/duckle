, raw as (
    select
      trim(coalesce(stdout, '')) as stdout_json,
      coalesce(stderr, '') as stderr_text,
      try_cast(exit_code as integer) as shell_exit_code,
      try_cast(duration_ms as bigint) as shell_duration_ms
    from input
  ),
  parsed as (
    select
      *,
      try_cast(nullif(stdout_json, '') as json) as payload
    from raw
  ),
  fields as (
    select
      json_extract_string(payload, '$.repo_key') as repo_key,
      json_extract_string(payload, '$.branch') as branch,
      json_extract_string(payload, '$.repo_path') as repo_path,
      json_extract_string(payload, '$.table_name') as table_name,
      coalesce(json_extract_string(payload, '$.previous_commit'), '') as previous_commit,
      coalesce(json_extract_string(payload, '$.head_commit'), '') as head_commit,
      coalesce(json_extract_string(payload, '$.export_mode'), '') as export_mode,
      coalesce(json_extract_string(payload, '$.reason'), '') as reason,
      coalesce(json_extract_string(payload, '$.stage_path'), '') as stage_path,
      coalesce(json_extract_string(payload, '$.snapshot_path'), '') as snapshot_path,
      coalesce(try_cast(json_extract_string(payload, '$.row_count') as bigint), 0) as row_count,
      coalesce(try_cast(json_extract_string(payload, '$.file_size_bytes') as bigint), 0) as file_size_bytes,
      coalesce(try_cast(json_extract_string(payload, '$.export_ok') as boolean), false) as export_ok,
      shell_exit_code,
      shell_duration_ms,
      stdout_json as raw_stdout,
      stderr_text as raw_stderr,
      payload is not null as parsed_ok
    from parsed
  )
  select
    *,
    coalesce(shell_exit_code, -1) = 0
      and parsed_ok
      and export_ok
      and table_name <> ''
      and head_commit <> ''
      and stage_path <> ''
      and file_size_bytes > 0 as export_result_ok,
    case
      when coalesce(shell_exit_code, -1) <> 0 then 'shell_failed'
      when not parsed_ok then 'parse_failed'
      when not export_ok then 'export_failed'
      when table_name = '' then 'missing_table'
      when head_commit = '' then 'missing_commit'
      when stage_path = '' then 'missing_stage_path'
      when file_size_bytes <= 0 then 'empty_export'
      else 'ready_to_publish'
    end as export_result_status
  from fields