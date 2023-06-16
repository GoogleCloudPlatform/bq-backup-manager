SELECT
    jsonPayload.global_run_id AS run_id,
    TIMESTAMP_MILLIS(CAST(SUBSTR(jsonPayload.global_run_id, 0, 13) AS INT64)) AS timestamp,
    jsonPayload.failed_dispatcher_entity_id AS entity_name,
    jsonPayload.failed_dispatcher_ex_name AS exception_name,
    jsonPayload.failed_dispatcher_ex_msg AS exception_message
FROM `${project}.${dataset}.${logging_table}`
    WHERE jsonPayload.failed_dispatcher_entity_id IS NOT NULL