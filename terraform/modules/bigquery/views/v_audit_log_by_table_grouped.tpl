SELECT
d.tablespec,
STRUCT(d.table_info.project, d.table_info.dataset, d.table_info.table) AS table_info,
ARRAY_AGG (
     STRUCT(
     d.run_rank,
     d.run_id,
     d.execution_ts,
     d.run_start_ts,
     d.tracking_id,
     d.is_dry_run,
     d.is_force_run,
     d.is_backup_time,
     d.backup_method,
     d.is_successful_run,
     d.run_has_retryable_error,
     d.failed_components,
     d.components_error_details,
     d.components_is_successful_details,
     d.components_is_retryable_error_details
   ) ORDER BY run_rank ASC, execution_ts DESC) AS runs

FROM `${project}.${dataset}.${v_audit_log_by_table}` d
GROUP BY d.tablespec, d.table_info.project, d.table_info.dataset, d.table_info.table