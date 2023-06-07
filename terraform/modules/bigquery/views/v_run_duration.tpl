SELECT
jsonPayload.global_run_id AS run_id
,TIMESTAMP_DIFF(max(timestamp),min(timestamp), MINUTE) AS run_duration_mins
FROM `${project}.${dataset}.${logging_table}`
GROUP BY 1
ORDER BY 1 DESC