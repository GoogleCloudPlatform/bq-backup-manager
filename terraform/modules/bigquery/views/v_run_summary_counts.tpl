WITH counts AS (
  SELECT
  run_id,
  timestamp,
  COUNT(DISTINCT tracking_id) AS total_count,
  SUM(CASE WHEN status = 'Success' THEN 1 ELSE 0 END) AS success_count,
  SUM(CASE WHEN status = 'Failed' THEN 1 ELSE 0 END) AS failed_count,
  SUM(CASE WHEN status = 'Retried' THEN 1 ELSE 0 END) AS retried_count,
  SUM(CASE WHEN is_backup_time THEN 1 ELSE 0 END) AS tables_ready_for_backup
  FROM ${project}.${dataset}.${v_run_summary}
  GROUP BY 1,2
)

SELECT
c.run_id,
c.timestamp AS run_id_timestamp,
c.total_count,
d.run_duration_mins,
STRUCT(
  c.success_count + c.failed_count AS complete_count,
  c.total_count - (c.success_count + c.failed_count) AS incomplete_count,
  CASE WHEN c.total_count > 0 THEN (c.success_count + c.failed_count) / c.total_count ELSE null END AS completion_coverage
) AS progress,
STRUCT(
  c.success_count,
  c.failed_count,
  c.retried_count,
  c.tables_ready_for_backup
) AS details,
STRUCT(
  (c.success_count + c.failed_count) AS total_count,
  (c.success_count + c.failed_count) - c.total_count AS variance
) AS cross_checks
FROM counts c
LEFT JOIN `${project}.${dataset}.${v_run_duration}` d
ON c.run_id = d.run_id
ORDER BY run_id DESC