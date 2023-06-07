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

package com.google.cloud.pso.bq_snapshot_manager.functions.f01_dispatcher;

import com.google.cloud.pso.bq_snapshot_manager.entities.JsonMessage;
import com.google.cloud.pso.bq_snapshot_manager.entities.NonRetryableApplicationException;
import com.google.cloud.pso.bq_snapshot_manager.entities.TableSpec;
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


public class Dispatcher {

    private final LoggingHelper logger;
    private final PubSubService pubSubService;
    private final ResourceScanner resourceScanner;
    private final DispatcherConfig config;
    private final PersistentSet persistentSet;
    private final String persistentSetObjectPrefix;
    private final Integer functionNumber;
    private final String runId;

    public Dispatcher(DispatcherConfig config,
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
                Dispatcher.class.getSimpleName(),
                functionNumber,
                config.getProjectId()
        );
    }

    public PubSubPublishResults execute(DispatcherRequest dispatcherRequest, String pubSubMessageId) throws IOException, NonRetryableApplicationException, InterruptedException {

        /*
           Check if we already processed this pubSubMessageId before to avoid re-running the dispatcher (and the whole process)
           in case we have unexpected errors with PubSub re-sending the message. This is an extra measure to avoid unnecessary cost.
           We do that by keeping simple flag files in GCS with the pubSubMessageId as file name.
         */
        String flagFileName = String.format("%s/%s", persistentSetObjectPrefix, pubSubMessageId);
        if (persistentSet.contains(flagFileName)) {
            // log error and ACK and return
            String msg = String.format("PubSub message ID '%s' has been processed before by the dispatcher. The message should be ACK to PubSub to stop retries. Please investigate further why the message was retried in the first place.",
                    pubSubMessageId);
            throw new NonRetryableApplicationException(msg);
        } else {
            logger.logInfoWithTracker(runId,
                    null,
                    String.format("Persisting processing key for PubSub message ID %s", pubSubMessageId));
            persistentSet.add(flagFileName);
        }

        // construct a BigQueryScopeLister using the input resourceScanner implementation
        BigQueryScopeLister bqScopeLister = new BigQueryScopeLister(
                resourceScanner,
                new LoggingHelper(
                        BigQueryScopeLister.class.getSimpleName(),
                        functionNumber,
                        config.getProjectId()
                ),
                runId
        );

        // List down which tables to publish a request for based on the input scan scope
        List<TableSpec> tablesInScope = bqScopeLister.listTablesInScope(dispatcherRequest.getBigQueryScope());

        // Convert each table in scope to a ConfiguratorRequest to be sent as a PubSub message
        List<JsonMessage> pubSubMessagesToPublish = new ArrayList<>();
        for (TableSpec tableSpec : tablesInScope) {
            pubSubMessagesToPublish.add(
                    new ConfiguratorRequest(
                            tableSpec,
                            runId,
                            TrackingHelper.generateTrackingId(runId),
                            dispatcherRequest.isDryRun(),
                            dispatcherRequest.isForceRun())
            );
        }

        // Publish the list of requests to PubSub
        PubSubPublishResults publishResults = pubSubService.publishTableOperationRequests(
                config.getProjectId(),
                config.getOutputTopic(),
                pubSubMessagesToPublish
        );

        // handle failed publishing requests
        for (FailedPubSubMessage msg : publishResults.getFailedMessages()) {
            ConfiguratorRequest request = (ConfiguratorRequest) msg.getMsg();

            String logMsg = String.format("Failed to publish this PubSub messages %s", msg.toString());
            logger.logWarnWithTracker(runId, request.getTargetTable(), logMsg);

            logger.logFailedDispatcherEntityId(
                    request.getTrackingId(),
                    request.getTargetTable(),
                    request.getTargetTable().toSqlString(),
                    msg.getException()
            );
        }

        // handle success publishing requests
        for (SuccessPubSubMessage msg : publishResults.getSuccessMessages()) {
            // this enable us to detect dispatched messages within a runId that fail in later stages (i.e. Tagger)
            ConfiguratorRequest request = (ConfiguratorRequest) msg.getMsg();

            logger.logSuccessDispatcherTrackingId(runId, request.getTrackingId(), request.getTargetTable());
        }

        logger.logFunctionEnd(runId, null);

        return publishResults;
    }


}
