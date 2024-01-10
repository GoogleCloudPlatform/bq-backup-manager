/*
 *
 *  * Copyright 2023 Google LLC
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     https://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.google.cloud.pso.bq_snapshot_manager.entities;

public enum ApplicationLog {
    // Used for generic logging event
    DEFAULT_LOG,
    // Used to log function start/stop
    TRACKER_LOG,
    // Used to log success dispatched table requests per run
    DISPATCHED_DATASET_REQUESTS_LOG,
    // Used to log failed dispatched requests per run
    FAILED_DISPATCHED_REQUESTS_LOG,
    // To capture trackers with non retryable exceptions during processing
    NON_RETRYABLE_EXCEPTIONS_LOG,
    // To capture trackers with retryable exceptions during processing
    RETRYABLE_EXCEPTIONS_LOG,
    // Unified log for service requests
    UNIFIED_LOG,
    // To capture counters and durations for dataset and table dispatchers
    DISPATCHER_COUNTERS_LOG,
}
