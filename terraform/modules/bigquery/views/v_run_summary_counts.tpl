WITH counts AS (
  SELECT
  run_id,
  timestamp,
  COUNT(DISTINCT tracking_id) AS dispatched_table_requests,
  SUM(CASE WHEN status = 'Success' THEN 1 ELSE 0 END) AS success_count,
  SUM(CASE WHEN status = 'Failed' THEN 1 ELSE 0 END) AS failed_count,
  FROM ${project}.${dataset}.${v_run_summary}
  GROUP BY 1,2
)

, backed_up_tables AS (

  SELECT
  r.run_id,
  COUNT(1) AS backed_up_tables_count
  FROM `${project}.${dataset}.${v_backed_up_tables}` r
  GROUP BY 1
)

SELECT
c.run_id,
c.timestamp AS run_id_timestamp,
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
  b.backed_up_tables_count
) AS details,
STRUCT(
  (c.success_count + c.failed_count) - c.dispatched_table_requests AS complete_vs_incomplete_variance
) AS cross_checks
FROM counts c
LEFT JOIN `${project}.${dataset}.${v_run_duration}` d
ON c.run_id = d.run_id
LEFT JOIN backed_up_tables b
ON b.run_id = d.run_id
ORDER BY run_id DESC