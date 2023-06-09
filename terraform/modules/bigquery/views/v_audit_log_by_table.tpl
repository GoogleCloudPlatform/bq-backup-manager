WITH dispatched_tables AS
(
SELECT DISTINCT
jsonPayload.dispatched_tablespec_project AS tablespec_project,
jsonPayload.dispatched_tablespec_dataset AS tablesspec_dataset,
jsonPayload.dispatched_tablespec_table AS tablespec_table,
jsonPayload.dispatched_tablespec AS tablespec,
FROM `${project}.${dataset}.${logging_table}`
WHERE jsonPayload.global_app_log = 'DISPATCHED_REQUESTS_LOG'
)
, configurator AS
(
SELECT
jsonPayload.unified_target_table AS tablespec,
jsonPayload.unified_run_id AS run_id,
jsonPayload.unified_tracking_id AS tracking_id,
CAST(jsonPayload.unified_is_successful AS BOOL) AS configurator_is_successful,
jsonPayload.unified_error AS configurator_error,
CAST(jsonPayload.unified_is_retryable_error AS BOOL) AS configurator_is_retryable_error,
CAST(JSON_VALUE(jsonPayload.unified_input_json, '$.isForceRun') AS BOOL) AS is_force_run,
CAST(JSON_VALUE(jsonPayload.unified_output_json, '$.isBackupCronTime') AS BOOL) AS is_backup_cron_time,
CAST(JSON_VALUE(jsonPayload.unified_output_json, '$.isTableCreatedBeforeTimeTravel') AS BOOL) AS is_table_created_before_time_travel,
CAST(JSON_VALUE(jsonPayload.unified_output_json, '$.isBackupTime') AS BOOL) AS is_backup_time,
JSON_VALUE(jsonPayload.unified_output_json, '$.backupPolicy.backup_method') AS backup_method,
CAST(JSON_VALUE(jsonPayload.unified_input_json, '$.isDryRun') AS BOOL) AS is_dry_run,
timestamp AS configurator_log_ts
FROM `${project}.${dataset}.${logging_table}`
WHERE jsonPayload.global_app_log = 'UNIFIED_LOG'
AND jsonPayload.unified_component = "2"
)
, bq_snapshoter AS
(
SELECT
jsonPayload.unified_tracking_id AS tracking_id,
CAST(jsonPayload.unified_is_successful AS BOOL) AS bq_snapshoter_is_successful,
jsonPayload.unified_error AS bq_snapshoter_error,
CAST(jsonPayload.unified_is_retryable_error AS BOOL) AS bq_snapshoter_is_retryable_error
FROM `${project}.${dataset}.${logging_table}`
WHERE jsonPayload.global_app_log = 'UNIFIED_LOG'
AND jsonPayload.unified_component = "3"
)
, gcs_snapshoter AS
(
SELECT
jsonPayload.unified_tracking_id AS tracking_id,
CAST(jsonPayload.unified_is_successful AS BOOL) AS gcs_snapshoter_is_successful,
jsonPayload.unified_error AS gcs_snapshoter_error,
CAST(jsonPayload.unified_is_retryable_error AS BOOL) AS gcs_snapshoter_is_retryable_error
FROM `${project}.${dataset}.${logging_table}`
WHERE jsonPayload.global_app_log = 'UNIFIED_LOG'
AND jsonPayload.unified_component = "-3"
)
, bq_tagger AS
(
SELECT
jsonPayload.unified_tracking_id AS tracking_id,
CAST(jsonPayload.unified_is_successful AS BOOL) AS tagger_bq_is_successful,
jsonPayload.unified_error AS tagger_bq_error,
CAST(jsonPayload.unified_is_retryable_error AS BOOL) AS tagger_bq_is_retryable_error,
FROM `${project}.${dataset}.${logging_table}`
WHERE jsonPayload.global_app_log = 'UNIFIED_LOG'
AND jsonPayload.unified_component = "4"
AND JSON_VALUE(jsonPayload.unified_input_json, '$.appliedBackupMethod') = "BIGQUERY_SNAPSHOT"
)
, gcs_tagger AS
(
SELECT
jsonPayload.unified_tracking_id AS tracking_id,
CAST(jsonPayload.unified_is_successful AS BOOL) AS tagger_gcs_is_successful,
jsonPayload.unified_error AS tagger_gcs_error,
CAST(jsonPayload.unified_is_retryable_error AS BOOL) AS tagger_gcs_is_retryable_error,
FROM `${project}.${dataset}.${logging_table}`
WHERE jsonPayload.global_app_log = 'UNIFIED_LOG'
AND jsonPayload.unified_component = "4"
AND JSON_VALUE(jsonPayload.unified_input_json, '$.appliedBackupMethod') = "GCS_SNAPSHOT"
)
, denormalized AS
(
SELECT
d.tablespec,
d.tablespec_project,
d.tablesspec_dataset,
d.tablespec_table,
RANK() OVER (PARTITION BY c.tablespec ORDER BY c.run_id DESC) AS run_rank,
c.run_id,
TIMESTAMP_MILLIS(CAST(SUBSTR(c.run_id, 0, 13) AS INT64)) AS run_start_ts,
c.configurator_log_ts,
c.tracking_id,
c.is_force_run,
c.is_backup_cron_time,
c.is_table_created_before_time_travel,
c.is_backup_time,
c.backup_method,
c.is_dry_run,

CASE
-- if configurator fails the whole run fails
WHEN  (NOT c.configurator_is_successful) THEN FALSE
-- if configurator finish successfully but it's not a backup time and no snapshoters should run
WHEN  (c.configurator_is_successful AND NOT c.is_backup_time) THEN TRUE
-- if It's BQ snapshot config then we expect the configurator, bq snasphoter and tagger (called by bq snapshoter) to run
WHEN c.backup_method = "BIGQUERY_SNAPSHOT" THEN (c.configurator_is_successful AND bs.bq_snapshoter_is_successful AND tbq.tagger_bq_is_successful)
-- If it's a GCS config then we expect the configurator, gcs snapshoter and tagger (called by gcs snapshoter to run)
WHEN c.backup_method = "GCS_SNAPSHOT" THEN (c.configurator_is_successful AND gs.gcs_snapshoter_is_successful AND tgs.tagger_gcs_is_successful)
-- If it's both config then we expect the configurator, both the GCS and BQ snapshoters, and tagger to run
WHEN c.backup_method = "BOTH" THEN (c.configurator_is_successful AND bs.bq_snapshoter_is_successful AND gs.gcs_snapshoter_is_successful AND tbq.tagger_bq_is_successful AND tgs.tagger_gcs_is_successful) END AS is_successful_run,

(COALESCE(c.configurator_is_retryable_error, FALSE) OR COALESCE(bs.bq_snapshoter_is_retryable_error, FALSE) OR COALESCE(gs.gcs_snapshoter_is_retryable_error, FALSE) OR COALESCE(tbq.tagger_bq_is_retryable_error, FALSE) OR COALESCE(tgs.tagger_gcs_is_retryable_error, FALSE)) AS run_has_retryable_error,

RTRIM(CONCAT(
(CASE WHEN c.configurator_error IS NOT NULL THEN 'configurator |' ELSE '' END),
(CASE WHEN bs.bq_snapshoter_error IS NOT NULL THEN 'bigquery_snapshoter |' ELSE '' END),
(CASE WHEN gs.gcs_snapshoter_error IS NOT NULL THEN 'gcs_snapshoter |' ELSE '' END),
(CASE WHEN tbq.tagger_bq_error IS NOT NULL THEN 'tagger_bigquery |' ELSE '' END),
(CASE WHEN tgs.tagger_gcs_error IS NOT NULL THEN 'tagger_gcs |' ELSE '' END)
), '|') AS failed_components,

STRUCT(c.configurator_error,bs.bq_snapshoter_error, gs.gcs_snapshoter_error,tbq.tagger_bq_error, tgs.tagger_gcs_error ) AS components_error_details,

STRUCT(c.configurator_is_successful,bs.bq_snapshoter_is_successful, gs.gcs_snapshoter_is_successful, tbq.tagger_bq_is_successful, tgs.tagger_gcs_is_successful) AS components_is_successful_details,

STRUCT(c.configurator_is_retryable_error,bs.bq_snapshoter_is_retryable_error,gs.gcs_snapshoter_is_retryable_error,tbq.tagger_bq_is_retryable_error, tgs.tagger_gcs_is_retryable_error) AS components_is_retryable_error_details

FROM dispatched_tables d
-- no tracking_id at this point, use the tablespec to get all runs for that tablespec
-- configurator is the first table-wise operation so we need to join on a successful run of it
LEFT JOIN configurator c ON d.tablespec = c.tablespec
-- then join using tracking_id
LEFT JOIN bq_snapshoter bs ON c.tracking_id = bs.tracking_id
LEFT JOIN gcs_snapshoter gs ON c.tracking_id = gs.tracking_id
LEFT JOIN bq_tagger tbq ON c.tracking_id = tbq.tracking_id
LEFT JOIN gcs_tagger tgs ON c.tracking_id = tgs.tracking_id
)
SELECT
d.tablespec,
STRUCT(d.tablespec_project AS project, d.tablesspec_dataset AS dataset, d.tablespec_table AS table) AS table_info,
d.run_rank,
d.run_id,
d.configurator_log_ts AS execution_ts,
d.run_start_ts,
d.tracking_id,
d.is_dry_run,
d.is_force_run,
d.is_backup_cron_time,
d.is_table_created_before_time_travel,
d.is_backup_time,
d.backup_method,
d.is_successful_run,
d.run_has_retryable_error,
CASE WHEN d.failed_components = '' THEN NULL ELSE d.failed_components END AS failed_components,
d.components_error_details,
d.components_is_successful_details,
d.components_is_retryable_error_details

FROM denormalized d








