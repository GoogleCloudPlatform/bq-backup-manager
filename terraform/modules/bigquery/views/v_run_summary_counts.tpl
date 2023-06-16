WITH counts AS (
  SELECT
  run_id,
  timestamp,
  COUNT(DISTINCT tracking_id) AS dispatched_table_requests,
  SUM(CASE WHEN status = 'Success' THEN 1 ELSE 0 END) AS success_count,
  SUM(CASE WHEN status = 'Failed' THEN 1 ELSE 0 END) AS failed_count,
  FROM `${project}.${dataset}.${v_run_summary}`
  GROUP BY 1,2
)

, dispatcher_errors AS (
  SELECT
    run_id,
    COUNT(1) dispatcher_errors
  FROM `${project}.${dataset}.${v_errors_non_retryable_dispatcher}`
  GROUP BY 1
)

, backed_up_tables AS (

  SELECT
  r.run_id,
  COUNT(1) AS backed_up_tables_count
  FROM `${project}.${dataset}.${v_backed_up_tables}` r
  GROUP BY 1
)

, tables_not_due_for_backup AS (
  SELECT
  run_id,
  COUNT(DISTINCT tracking_id) tables_not_due_for_backup
  FROM `${project}.${dataset}.${v_audit_log_by_table}`
  WHERE NOT is_backup_time
  GROUP BY 1
)

SELECT
c.run_id,
c.timestamp AS run_id_timestamp,
de.dispatcher_errors,
c.dispatched_table_requests,
d.run_duration_mins,
STRUCT(
  c.success_count  + c.failed_count AS completed_requests,
  c.dispatched_table_requests - (c.success_count + c.failed_count) AS incomplete_requests,
  CASE WHEN c.dispatched_table_requests > 0 THEN (c.success_count + c.failed_count) / c.dispatched_table_requests ELSE null END AS completion_coverage
) AS progress,
STRUCT(
  c.success_count,
  c.failed_count,
  b.backed_up_tables_count,
  nb.tables_not_due_for_backup
) AS details,
STRUCT(
  (c.success_count + c.failed_count) - c.dispatched_table_requests AS complete_vs_incomplete_variance
) AS cross_checks
FROM counts c
LEFT JOIN `${project}.${dataset}.${v_run_duration}` d
ON c.run_id = d.run_id
LEFT JOIN backed_up_tables b
ON b.run_id = d.run_id
LEFT JOIN dispatcher_errors de
ON c.run_id = de.run_id
LEFT JOIN tables_not_due_for_backup nb
ON c.run_id = nb.run_id
ORDER BY run_id DESC