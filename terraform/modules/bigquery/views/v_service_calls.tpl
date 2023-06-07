SELECT
run_id,
tracker,
table_spec,
SUM(CASE WHEN step = 'START'  AND function_number = 2 THEN 1 ELSE 0 END) AS configurator_starts,
SUM(CASE WHEN step = 'END' AND function_number = 2 THEN 1 ELSE 0 END) AS configurator_ends,
SUM(CASE WHEN step = 'START'  AND function_number = 3 THEN 1 ELSE 0 END) AS bq_snapshoter_starts,
SUM(CASE WHEN step = 'END' AND function_number = 3 THEN 1 ELSE 0 END) AS bq_snapshoter_ends,
SUM(CASE WHEN step = 'START'  AND function_number = -3 THEN 1 ELSE 0 END) AS gcs_snapshoter_starts,
SUM(CASE WHEN step = 'END' AND function_number = -3 THEN 1 ELSE 0 END) AS gcs_snapshoter_ends,
SUM(CASE WHEN step = 'START'  AND function_number = 4 THEN 1 ELSE 0 END) AS tagger_starts,
SUM(CASE WHEN step = 'END' AND function_number = 4 THEN 1 ELSE 0 END) AS tagger_ends
FROM
`${project}.${dataset}.${logging_view_steps}`
WHERE function_number > 1
GROUP BY 1,2,3