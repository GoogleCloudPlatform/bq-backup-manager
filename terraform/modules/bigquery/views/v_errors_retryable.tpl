WITH retryable AS
(
SELECT
jsonPayload.global_run_id AS run_id,
TIMESTAMP_MILLIS(CAST(SUBSTR(jsonPayload.global_run_id, 0, 13) AS INT64)) AS run_start_timestamp,
jsonPayload.retryable_ex_tracking_id AS tracking_id,
STRUCT(
CONCAT(jsonPayload.global_tablespec_project,'.',jsonPayload.global_tablespec_dataset,'.',jsonPayload.global_tablespec_table) AS spec,
jsonPayload.global_tablespec_project AS project,
jsonPayload.global_tablespec_dataset AS dataset,
jsonPayload.global_tablespec_table AS table
) AS table_info,
resource.labels.service_name AS service_name,
jsonPayload.retryable_ex_name AS exception_name,
jsonPayload.retryable_ex_msg AS exception_message,
jsonPayload.retryable_ex_code AS exception_code,
jsonPayload.retryable_ex_reason AS exception_reason,
COUNT(1) count
FROM
`${project}.${dataset}.${logging_table}`
WHERE
jsonPayload.global_app_log = 'RETRYABLE_EXCEPTIONS_LOG'
GROUP BY
jsonPayload.global_run_id,
jsonPayload.retryable_ex_tracking_id,
jsonPayload.global_tablespec_project,
jsonPayload.global_tablespec_dataset,
jsonPayload.global_tablespec_table,
resource.labels.service_name,
jsonPayload.retryable_ex_name,
jsonPayload.retryable_ex_msg,
jsonPayload.retryable_ex_code,
jsonPayload.retryable_ex_reason

)

SELECT *  FROM retryable