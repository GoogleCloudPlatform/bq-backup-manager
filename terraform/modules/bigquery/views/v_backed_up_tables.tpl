SELECT
  DISTINCT
  r.run_id,
  r.tracking_id,
  r.table_info.project,
  r.table_info.dataset,
  r.table_info.table,
  r.tablespec,
  r.backup_method,
FROM `${project}.${dataset}.${v_audit_log_by_table}` r
WHERE is_successful_run AND r.is_backup_time