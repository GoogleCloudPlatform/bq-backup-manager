WITH counts AS (
SELECT
  r.run_id,
  -- one tracking id might have multiple records/runs due to retryable errors
  r.tracking_id,
  TIMESTAMP_MILLIS(CAST(SUBSTR(r.run_id, 0, 13) AS INT64)) AS timestamp,
  -- if at least one run is successful then the tracking id has completed successfully
  SUM(CASE WHEN r.is_successful_run IS TRUE THEN 1 ELSE 0 END) AS success_count,
  -- if at least one run is not successully and is not retryable then the tracking id has completed with a failure
  SUM(CASE WHEN r.is_successful_run IS FALSE AND r.run_has_retryable_error IS FALSE THEN 1 ELSE 0 END) AS non_retryable_failure_count,
  -- runs that are not success but failed for retryable errors
  SUM(CASE WHEN r.is_successful_run IS FALSE AND r.run_has_retryable_error IS TRUE THEN 1 ELSE 0 END) AS retryable_failure_count,
FROM `${project}.${dataset}.${v_unified_logging}` r
GROUP BY 1,2
)

SELECT
run_id,
timestamp,
tracking_id,
CASE WHEN success_count > 0 THEN 'Success'
 WHEN non_retryable_failure_count > 0 THEN 'Failed'
 WHEN retryable_failure_count > 0 THEN 'Retried' END AS status
FROM counts