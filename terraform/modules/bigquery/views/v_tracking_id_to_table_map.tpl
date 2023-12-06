SELECT
runId AS run_id,
trackingId AS tracking_id,
targetTable.project AS project_id,
targetTable.dataset AS dataset_id,
targetTable.table AS table_id,
CONCAT(targetTable.project,'.', targetTable.dataset, '.', targetTable.table) AS tablespec
FROM
`${project}.${dataset}.${dispatched_tables}`