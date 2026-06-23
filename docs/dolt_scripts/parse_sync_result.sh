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
      json_extract_string(payload, '$.remote_url') as remote_url,
      json_extract_string(payload, '$.branch') as branch,
      json_extract_string(payload, '$.repo_path') as repo_path,
      coalesce(json_extract_string(payload, '$.previous_commit'), '') as previous_commit,
      coalesce(json_extract_string(payload, '$.head_commit'), '') as head_commit,
      coalesce(
        try_cast(json_extract_string(payload, '$.should_skip') as boolean),
        false
      ) as should_skip,
      shell_exit_code,
      shell_duration_ms,
      stdout_json as raw_stdout,
      stderr_text as raw_stderr,
      payload is not null as parsed_ok
    from parsed
  )
  select
    *,
    coalesce(shell_exit_code, -1) = 0 and parsed_ok as sync_ok,
    case
      when coalesce(shell_exit_code, -1) <> 0 then 'shell_failed'
      when not parsed_ok then 'parse_failed'
      when should_skip then 'unchanged'
      else 'changed'
    end as sync_status
  from fields