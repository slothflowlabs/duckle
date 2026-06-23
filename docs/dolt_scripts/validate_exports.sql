, checks as (
    select
      *,
      coalesce(export_result_ok, false) as check_export_result_ok,
      coalesce(stage_path, '') <> '' as check_stage_path_present,
      coalesce(snapshot_path, '') <> '' as check_snapshot_path_present,
      coalesce(table_name, '') <> '' as check_table_name_present,
      coalesce(head_commit, '') <> '' as check_head_commit_present,
      coalesce(row_count, 0) > 0 as check_row_count_positive,
      coalesce(file_size_bytes, 0) > 0 as check_file_size_positive
    from input
  ),
  scored as (
    select
      *,
      check_export_result_ok
        and check_stage_path_present
        and check_snapshot_path_present
        and check_table_name_present
        and check_head_commit_present
        and check_row_count_positive
        and check_file_size_positive as export_validation_ok
    from checks
  )
  select
    *,
    case
      when not check_export_result_ok then 'export_result_not_ok'
      when not check_stage_path_present then 'missing_stage_path'
      when not check_snapshot_path_present then 'missing_snapshot_path'
      when not check_table_name_present then 'missing_table_name'
      when not check_head_commit_present then 'missing_head_commit'
      when not check_row_count_positive then 'row_count_not_positive'
      when not check_file_size_positive then 'file_size_not_positive'
      else 'valid'
    end as export_validation_status
  from scored