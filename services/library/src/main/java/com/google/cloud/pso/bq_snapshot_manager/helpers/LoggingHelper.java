/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.pso.bq_snapshot_manager.helpers;

import com.google.cloud.pso.bq_snapshot_manager.entities.*;
import com.google.flatbuffers.Table;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import javax.annotation.Nullable;
import javax.swing.text.StyledEditorKit;
import java.util.Arrays;
import java.util.stream.Stream;

import static net.logstash.logback.argument.StructuredArguments.kv;

public class LoggingHelper {

    private final Logger logger;
    private final String loggerName;
    private final Integer functionNumber;
    // Used to create a trace
    private final String projectId;

    public LoggingHelper(String loggerName, Integer functionNumber, String projectId) {
        this.loggerName = loggerName;
        this.functionNumber = functionNumber;
        this.projectId = projectId;

        logger = LoggerFactory.getLogger(loggerName);
    }

    public void logDebugWithTracker(Boolean isDryRun, String tracker, @Nullable TableSpec tableSpec, String msg) {
        logWithTracker(ApplicationLog.DEFAULT_LOG, isDryRun, tableSpec, tracker, msg, Level.DEBUG);
    }

    public void logDebugWithTracker(String tracker, @Nullable TableSpec tableSpec, String msg) {
        logWithTracker(ApplicationLog.DEFAULT_LOG, null, tableSpec, tracker, msg, Level.DEBUG);
    }

    public void logInfoWithTracker(Boolean isDryRun, String tracker, @Nullable TableSpec tableSpec, String msg) {
        logWithTracker(ApplicationLog.DEFAULT_LOG, isDryRun, tableSpec, tracker, msg, Level.INFO);
    }

    public void logInfoWithTracker(String tracker, @Nullable TableSpec tableSpec, String msg) {
        logWithTracker(ApplicationLog.DEFAULT_LOG, null, tableSpec, tracker, msg, Level.INFO);
    }

    public void logWarnWithTracker(Boolean isDryRun, String tracker, @Nullable TableSpec tableSpec,String msg) {
        logWithTracker(ApplicationLog.DEFAULT_LOG, isDryRun, tableSpec, tracker, msg, Level.WARN);
    }

    public void logWarnWithTracker( String tracker, @Nullable TableSpec tableSpec,String msg) {
        logWithTracker(ApplicationLog.DEFAULT_LOG, null, tableSpec, tracker, msg, Level.WARN);
    }

    public void logSevereWithTracker(Boolean isDryRun, String tracker, @Nullable TableSpec tableSpec, String msg) {
        logWithTracker(ApplicationLog.DEFAULT_LOG, isDryRun, tableSpec, tracker, msg, Level.ERROR);
    }

    public void logSevereWithTracker(String tracker, @Nullable TableSpec tableSpec, String msg) {
        logWithTracker(ApplicationLog.DEFAULT_LOG, null, tableSpec, tracker, msg, Level.ERROR);
    }

    private void logWithTracker(ApplicationLog log, Boolean isDryRun, @Nullable TableSpec tableSpec, String tracker, String msg, Level level) {
        logWithTracker(log,isDryRun, tracker, tableSpec, msg, level, new Object[]{});
    }

    public void logSuccessDispatcherTrackingId(String trackingId, String dispatchedTrackingId, TableSpec tableSpec) {

        Object [] attributes = new Object[]{
                kv("dispatched_tracking_id", dispatchedTrackingId),
                kv("dispatched_tablespec", tableSpec.toSqlString()),
                kv("dispatched_tablespec_project", tableSpec.getProject()),
                kv("dispatched_tablespec_dataset", tableSpec.getDataset()),
                kv("dispatched_tablespec_table", tableSpec.getTable()),
        };

        logWithTracker(
                ApplicationLog.DISPATCHED_REQUESTS_LOG,
                null,
                trackingId,
                tableSpec,
                String.format("Dispatched request for table '%s' with trackindId `%s`", tableSpec.toSqlString(), dispatchedTrackingId),
                Level.INFO,
                attributes
        );
    }

    // To log failed processing of projects, datasets or tables
    public void logFailedDispatcherEntityId(String trackingId, @Nullable TableSpec tableSpec, String entityId, Exception ex) {

        Object [] attributes = new Object[]{
                kv("failed_dispatcher_entity_id", entityId),
                kv("failed_dispatcher_ex_name", ex.getClass().getName()),
                kv("failed_dispatcher_ex_msg", ex.getMessage())
        };

        logWithTracker(
                ApplicationLog.FAILED_DISPATCHED_REQUESTS_LOG,
                null,
                trackingId,
                tableSpec,
                String.format("Failed to process entity `%s`.Exception: %s. Msg: %s",
                        entityId,
                        ex.getClass().getName(),
                        ex.getMessage()
                        ),
                Level.ERROR,
                attributes
        );
    }

    // To log failed processing of projects, datasets or tables
    public void logNonRetryableExceptions(String trackingId, @Nullable TableSpec tableSpec, Exception ex) {

        Object [] attributes = new Object[]{
                kv("non_retryable_ex_tracking_id", trackingId),
                kv("non_retryable_ex_name", ex.getClass().getName()),
                kv("non_retryable_ex_msg", ExceptionUtils.getStackTrace(ex)),
        };

        logWithTracker(
                ApplicationLog.NON_RETRYABLE_EXCEPTIONS_LOG,
                null,
                trackingId,
                tableSpec,
                String.format("Caught a Non-Retryable exception while processing tracker `%s`. Exception: %s. Msg: %s", trackingId, ex.getClass().getName(), ex.getMessage()),
                Level.ERROR,
                attributes
        );
        ex.printStackTrace();
    }

    // To log failed processing of projects, datasets or tables
    public void logRetryableExceptions(String trackingId, @Nullable  TableSpec tableSpec, Exception ex, String reason) {

        Object [] attributes = new Object[]{
                kv("retryable_ex_tracking_id", trackingId),
                kv("retryable_ex_name", ex.getClass().getName()),
                kv("retryable_ex_msg", ExceptionUtils.getStackTrace(ex)),
                kv("retryable_ex_reason", reason),
        };

        logWithTracker(
                ApplicationLog.RETRYABLE_EXCEPTIONS_LOG,
                null,
                trackingId,
                tableSpec,
                String.format("Caught a Retryable exception while processing tracker `%s`. Exception: %s. Msg: %s. Classification Reason: %s.",
                        trackingId,
                        ex.getClass().getName(),
                        ex.getMessage(),
                        reason
                ),
                Level.WARN,
                attributes
        );
        ex.printStackTrace();
    }

    public void logFunctionStart(String trackingId, @Nullable TableSpec tableSpec) {
        logFunctionLifeCycleEvent(trackingId, tableSpec, FunctionLifeCycleEvent.START);
    }

    public void logFunctionEnd(String trackingId, @Nullable TableSpec tableSpec) {
        logFunctionLifeCycleEvent( trackingId, tableSpec, FunctionLifeCycleEvent.END);
    }

    private void logFunctionLifeCycleEvent(String trackingId, @Nullable TableSpec tableSpec, FunctionLifeCycleEvent event) {

        Object [] attributes = new Object[]{
                kv("function_lifecycle_event", event),
                kv("function_lifecycle_functionNumber", functionNumber),
        };

        logWithTracker(
                ApplicationLog.TRACKER_LOG,
                null,
                trackingId,
                tableSpec,
                String.format("%s | %s | %s | %s",
                        loggerName,
                        functionNumber,
                        event,
                        tableSpec == null? null: tableSpec.toSqlString()
                        ),
                Level.INFO,
                attributes
        );

    }


    public void logUnified(
            Boolean isDryRun,
            String component,
            String runId,
            String trackingId,
            @Nullable TableSpec targetTable,
            @Nullable JsonMessage inputJson,
            @Nullable JsonMessage outputJson,
            boolean isSuccess,
            @Nullable Exception exception,
            boolean isRetryableError
    ){

        Object [] attributes = new Object[]{
                kv("unified_component", component),
                kv("unified_run_id", runId),
                kv("unified_tracking_id", trackingId),
                kv("unified_target_table", targetTable != null? targetTable.toSqlString(): null),
                kv("unified_input_json", inputJson != null? inputJson.toJsonString(): null),
                kv("unified_output_json", outputJson != null? outputJson.toJsonString(): null),
                kv("unified_is_successful", String.valueOf(isSuccess)),
                kv("unified_error", exception != null? ExceptionUtils.getStackTrace(exception) : null),
                kv("unified_is_retryable_error", String.valueOf(isRetryableError))
        };

        logWithTracker(
                ApplicationLog.UNIFIED_LOG,
                isDryRun,
                trackingId,
                targetTable,
                String.format("Unified log event for component '%s' processing target table '%s' with isSuccess status = '%s' and isRetryable = '%s'",
                        component,
                        targetTable != null? targetTable.toSqlString(): "",
                        isSuccess,
                        isRetryableError
                ),
                isSuccess || isRetryableError? Level.INFO : Level.ERROR,
                attributes
        );
    }

    private void logWithTracker(ApplicationLog log, Boolean isDryRun, String tracker, @Nullable TableSpec tableSpec, String msg, Level level, Object [] extraAttributes) {

        // Enable JSON logging with Logback and SLF4J by enabling the Logstash JSON Encoder in your logback.xml configuration.

        String payload = String.format("%s | %s | %s | %s | %s | %s",
                Globals.APPLICATION_NAME,
                log,
                loggerName,
                isDryRun!=null? (isDryRun?"Dry-Run":"Wet-Run") : null,
                tracker,
                msg
        );

        String runId;
        try{
            runId = TrackingHelper.parseRunIdAsPrefix(tracker);
        }catch (Exception e){
            // so that it never appears in max(run_id) queries
            runId = TrackingHelper.MIN_RUN_ID;
        }

        Object [] globalAttributes = new Object[]{
                kv("global_app", Globals.APPLICATION_NAME),
                kv("global_logger_name", this.loggerName),
                kv("global_app_log", log),
                kv("global_tracker", tracker),
                kv("global_is_dry_run", isDryRun == null? null: isDryRun.toString()),
                kv("global_tablespec_project", tableSpec == null? null: tableSpec.getProject()),
                kv("global_tablespec_dataset", tableSpec == null? null: tableSpec.getDataset()),
                kv("global_tablespec_table", tableSpec == null? null: tableSpec.getTable()),
                kv("global_run_id", runId),
                kv("global_msg", msg),
                kv("severity", level.toString()),

                // Group all log entries with the same tracker in CLoud Logging iew
                kv("logging.googleapis.com/trace",
                        String.format("projects/%s/traces/%s", projectId, tracker))
        };

        // setting the "severity" KV will override the logger.<severity>
        logger.info(
                payload,
                Stream.concat(
                        Arrays.stream(globalAttributes),
                        Arrays.stream(extraAttributes)
                ).toArray()
        );
    }
}
