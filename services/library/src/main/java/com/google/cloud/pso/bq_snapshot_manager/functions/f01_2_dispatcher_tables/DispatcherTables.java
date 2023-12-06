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

package com.google.cloud.pso.bq_snapshot_manager.functions.f01_2_dispatcher_tables;

import com.google.cloud.Timestamp;
import com.google.cloud.Tuple;
import com.google.cloud.pso.bq_snapshot_manager.entities.GlobalVariables;
import com.google.cloud.pso.bq_snapshot_manager.entities.JsonMessage;
import com.google.cloud.pso.bq_snapshot_manager.entities.NonRetryableApplicationException;
import com.google.cloud.pso.bq_snapshot_manager.entities.TableSpec;
import com.google.cloud.pso.bq_snapshot_manager.functions.f01_1_dispatcher.BigQueryScopeLister;
import com.google.cloud.pso.bq_snapshot_manager.functions.f01_1_dispatcher.DispatcherConfig;
import com.google.cloud.pso.bq_snapshot_manager.functions.f02_configurator.ConfiguratorRequest;
import com.google.cloud.pso.bq_snapshot_manager.helpers.LoggingHelper;
import com.google.cloud.pso.bq_snapshot_manager.helpers.TrackingHelper;
import com.google.cloud.pso.bq_snapshot_manager.helpers.Utils;
import com.google.cloud.pso.bq_snapshot_manager.services.pubsub.FailedPubSubMessage;
import com.google.cloud.pso.bq_snapshot_manager.services.pubsub.PubSubPublishResults;
import com.google.cloud.pso.bq_snapshot_manager.services.pubsub.PubSubService;
import com.google.cloud.pso.bq_snapshot_manager.services.pubsub.SuccessPubSubMessage;
import com.google.cloud.pso.bq_snapshot_manager.services.scan.*;
import com.google.cloud.pso.bq_snapshot_manager.services.set.PersistentSet;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class DispatcherTables {

    private final LoggingHelper logger;
    private final PubSubService pubSubService;
    private final ResourceScanner resourceScanner;
    private final DispatcherConfig config;
    private final PersistentSet persistentSet;
    private final String persistentSetObjectPrefix;
    private final Integer functionNumber;
    private final String runId;

    public DispatcherTables(DispatcherConfig config,
                            PubSubService pubSubService,
                            ResourceScanner resourceScanner,
                            PersistentSet persistentSet,
                            String persistentSetObjectPrefix,
                            Integer functionNumber,
                            String runId) {

        this.config = config;
        this.pubSubService = pubSubService;
        this.resourceScanner = resourceScanner;
        this.persistentSet = persistentSet;
        this.persistentSetObjectPrefix = persistentSetObjectPrefix;
        this.functionNumber = functionNumber;
        this.runId = runId;

        logger = new LoggingHelper(
                DispatcherTables.class.getSimpleName(),
                functionNumber,
                config.getProjectId(),
                config.getApplicationName()
        );
    }

    public PubSubPublishResults execute(DispatcherTableRequest dispatcherRequest, String pubSubMessageId) throws IOException, NonRetryableApplicationException, InterruptedException {

        long functionStartTs = System.currentTimeMillis();
        /*
           Check if we already processed this pubSubMessageId before to avoid re-running the dispatcher (and the whole process)
           in case we have unexpected errors with PubSub re-sending the message. This is an extra measure to avoid unnecessary cost.
           We do that by keeping simple flag files in GCS with the pubSubMessageId as file name.
         */
        String flagFileName = String.format("%s/%s", persistentSetObjectPrefix, pubSubMessageId);
        if (persistentSet.contains(flagFileName)) {
            // log error and ACK and return
            String msg = String.format("PubSub message ID '%s' has been processed before by the tables dispatcher. " +
                            "This is probably retried by PubSub due to it's subscription_ack_deadline_seconds being" +
                            " 10min or less (max) while the dispatcher process is taking more. The message is not " +
                            "going to be re-processed and ignored instead.",
                    pubSubMessageId);
            logger.logWarnWithTracker(dispatcherRequest.isDryRun(), dispatcherRequest.getRunId(), null, msg);

            // end execution successfully and the controller will ACK the request to PubSub and stop retries
            return new PubSubPublishResults(new ArrayList<>(0), new ArrayList<>(0));
        } else {
            logger.logInfoWithTracker(runId,
                    null,
                    String.format("Persisting processing key for PubSub message ID %s", pubSubMessageId));
            persistentSet.add(flagFileName);
        }

        // list down all tables in the dataset
        List<String> tablesInclusionList;

        long listingStartTs = System.currentTimeMillis();
        logger.logInfoWithTracker(runId, null, "Starting to list and filter tables in scope..");

        // tables to process are explict passed as tablesInclusionList from the original BigQueryScanScope or
        // implicitly via the project and dataset info
        if (dispatcherRequest.getTablesExclusionList() == null || dispatcherRequest.getTablesExclusionList().isEmpty()){
            tablesInclusionList = resourceScanner.listTables(
                    dispatcherRequest.getDatasetSpec().getProject(),
                    dispatcherRequest.getDatasetSpec().getDataset());
        }else{
            tablesInclusionList = dispatcherRequest.getTablesInclusionList();
        }

        // extract the exclusion REGEX elements from the exclusion list
        List<Pattern> tableExcludeListPatterns = Utils.extractAndCompilePatterns(
                dispatcherRequest.getTablesExclusionList(),
                GlobalVariables.REGEX_PREFIX);

        List<TableSpec> tablesInScope = new ArrayList<>();

        // filter the tables in the dataset by excluding the ones with matches in the exclusion list
        for (String table : tablesInclusionList) {
            TableSpec tableSpec = TableSpec.fromSqlString(table);
            try {
                Tuple<Boolean, String> checkResults = Utils.isElementMatchLiteralOrRegexList(table,
                        dispatcherRequest.getTablesExclusionList(),
                        tableExcludeListPatterns);
                if (!checkResults.x()) {
                    tablesInScope.add(tableSpec);
                } else {
                    logger.logInfoWithTracker(runId,
                            tableSpec,
                            String.format("Table %s is excluded by %s", table, checkResults.y()));
                }
            } catch (Exception ex) {
                // log and continue
                logger.logFailedDispatcherEntityId(runId, tableSpec, table, ex.getMessage(), ex.getClass().getName());
            }
        }

        long listingEndTs = System.currentTimeMillis();
        logger.logInfoWithTracker(runId,
                null,
                String.format("Finished listing and filtering down tables in-scope in %s ms.", listingEndTs-listingStartTs));

        // publish the tables in scope to the Configurator

        // Convert each table in scope to a ConfiguratorRequest to be sent as a PubSub message
        List<JsonMessage> pubSubMessagesToPublish = new ArrayList<>();
        // use the start time of this run as a reference point in time for CRON checks across all requests in this run
        Timestamp refTs = TrackingHelper.parseRunIdAsTimestamp(runId);
        for (TableSpec tableSpec : tablesInScope) {
            pubSubMessagesToPublish.add(
                    new ConfiguratorRequest(
                            tableSpec,
                            runId,
                            TrackingHelper.generateTrackingId(runId),
                            dispatcherRequest.isDryRun(),
                            dispatcherRequest.isForceRun(),
                            refTs
                    )
            );
        }

        long publishingStartTs = System.currentTimeMillis();
        logger.logInfoWithTracker(runId, null,
                String.format("Starting to publish to PubSub: %s messages", pubSubMessagesToPublish.size())
        );

        // Publish the list of requests to PubSub
        PubSubPublishResults publishResults = pubSubService.publishTableOperationRequests(
                config.getProjectId(),
                config.getOutputTopic(),
                pubSubMessagesToPublish
        );

        long publishingEndTs = System.currentTimeMillis();
        logger.logInfoWithTracker(runId, null,
                String.format("Finished publishing to PubSub in %s ms: %s success messages, %s failed messages",
                        publishingEndTs - publishingStartTs,
                        publishResults.getSuccessMessages().size(),
                        publishResults.getFailedMessages().size())
        );

        // handle failed publishing requests
        for (FailedPubSubMessage msg : publishResults.getFailedMessages()) {
            ConfiguratorRequest request = (ConfiguratorRequest) msg.getMsg();

            String logMsg = String.format("Failed to publish this PubSub messages %s", msg);
            logger.logWarnWithTracker(runId, request.getTargetTable(), logMsg);

            logger.logFailedDispatcherEntityId(
                    request.getTrackingId(),
                    request.getTargetTable(),
                    request.getTargetTable().toSqlString(),
                    msg.getExceptionMessage(),
                    msg.getExceptionClass()
            );
        }

        long dispatcherLogStartTs = System.currentTimeMillis();
        logger.logInfoWithTracker(runId, null, "Starting to list creating dispatched tables log..");

        // handle success publishing requests
        for (SuccessPubSubMessage msg : publishResults.getSuccessMessages()) {
            // this enable us to detect dispatched messages within a runId that fail in later stages (i.e. Tagger)
            ConfiguratorRequest request = (ConfiguratorRequest) msg.getMsg();

            logger.logSuccessDispatcherTrackingId(runId, request.getTrackingId(), request.getTargetTable());
        }

        long dispatcherLogEndTs = System.currentTimeMillis();
        logger.logInfoWithTracker(runId,
                null,
                String.format("Finished creating dispatched  tables log in %s ms.", listingEndTs-listingStartTs));

        String timersJson = "Execution Timers (ms): {'total': %s, 'listing_scope': %s, 'publish_to_pubsub': %s, 'create_dispatcher_log': %s}";
        logger.logInfoWithTracker(runId, null,
                String.format(timersJson,
                        dispatcherLogEndTs - functionStartTs,
                        listingEndTs - listingStartTs,
                        publishingEndTs - publishingStartTs,
                        dispatcherLogEndTs - dispatcherLogStartTs
                )
        );

        logger.logFunctionEnd(runId, null);

        return publishResults;
    }


}
