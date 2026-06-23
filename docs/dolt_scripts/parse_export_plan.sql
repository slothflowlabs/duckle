, raw as (
    select
      trim(coalesce(stdout, '')) as stdout_text,
      coalesce(stderr, '') as stderr_text,
      try_cast(exit_code as integer) as shell_exit_code,
      try_cast(duration_ms as bigint) as shell_duration_ms
    from input
  ),
  lines as (
    select
      row_number() over () as plan_row_number,
      trim(line) as plan_json,
      stderr_text,
      shell_exit_code,
      shell_duration_ms,
      false as synthetic_error
    from raw,
      unnest(string_split(stdout_text, chr(10))) as t(line)
    where trim(line) <> ''

    union all

    select
      1 as plan_row_number,
      '' as plan_json,
      stderr_text,
      shell_exit_code,
      shell_duration_ms,
      true as synthetic_error
    from raw
    where stdout_text = ''
  ),
  parsed as (
    select
      *,
      case
        when synthetic_error then null
        else try_cast(plan_json as json)
      end as payload
    from lines
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
      coalesce(json_extract_string(payload, '$.delta_path'), '') as delta_path,
      coalesce(
        try_cast(json_extract_string(payload, '$.should_export') as boolean),
        false
      ) as should_export,
      plan_row_number,
      plan_json,
      stderr_text as raw_stderr,
      shell_exit_code,
      shell_duration_ms,
      payload is not null as parsed_ok
    from parsed
  )
  select
    *,
    coalesce(shell_exit_code, -1) = 0
      and parsed_ok
      and table_name <> ''
      and head_commit <> ''
      and export_mode <> '' as plan_ok,
    case
      when coalesce(shell_exit_code, -1) <> 0 then 'shell_failed'
      when not parsed_ok then 'parse_failed'
      when table_name = '' then 'missing_table'
      when head_commit = '' then 'missing_commit'
      when export_mode = '' then 'missing_export_mode'
      when should_export then export_mode
      else 'skip'
    end as plan_status
  from fields