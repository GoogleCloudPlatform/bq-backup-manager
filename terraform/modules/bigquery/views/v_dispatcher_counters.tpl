SELECT
jsonPayload.global_run_id,
jsonPayload.global_is_dry_run,
jsonPayload.dispatcher_counters_dispatcher_type,
jsonPayload.dispatcher_counters_entity_spec,
jsonPayload.dispatcher_counters_entities_count,
jsonPayload.dispatcher_counters_total_ms,
jsonPayload.dispatcher_counters_listing_ms,
jsonPayload.dispatcher_counters_pubsub_publishing_ms,
jsonPayload.dispatcher_counters_create_dispatcher_log_ms,
FROM `${project}.${dataset}.${logging_table}`
WHERE jsonPayload.global_app_log = 'DISPATCHER_COUNTERS_LOG'