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

package com.google.cloud.pso.bq_snapshot_manager.functions.f01_1_dispatcher;


import com.google.cloud.pso.bq_snapshot_manager.entities.DatasetSpec;
import com.google.cloud.pso.bq_snapshot_manager.entities.JsonMessage;
import com.google.cloud.pso.bq_snapshot_manager.entities.NonRetryableApplicationException;
import com.google.cloud.pso.bq_snapshot_manager.functions.f01_2_dispatcher_tables.DispatcherTableRequest;
import com.google.cloud.pso.bq_snapshot_manager.helpers.LoggingHelper;
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

  public Dispatcher(
      DispatcherConfig config,
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

    logger =
        new LoggingHelper(
            Dispatcher.class.getSimpleName(),
            functionNumber,
            config.getProjectId(),
            config.getApplicationName());
  }

  public PubSubPublishResults execute(DispatcherRequest dispatcherRequest, String pubSubMessageId)
      throws IOException, NonRetryableApplicationException, InterruptedException {

    long functionStartTs = System.currentTimeMillis();
    /*
      Check if we already processed this pubSubMessageId before to avoid re-running the dispatcher (and the whole process)
      in case we have unexpected errors with PubSub re-sending the message. This is an extra measure to avoid unnecessary cost.
      We do that by keeping simple flag files in GCS with the pubSubMessageId as file name.
    */
    String flagFileName = String.format("%s/%s", persistentSetObjectPrefix, pubSubMessageId);
    if (persistentSet.contains(flagFileName)) {
      // log error and ACK and return
      String msg =
          String.format(
              "PubSub message ID '%s' has been processed before by the tables dispatcher. "
                  + "This is probably retried by PubSub due to it's subscription_ack_deadline_seconds being"
                  + " 10min or less (max) while the dispatcher process is taking more. The message is not "
                  + "going to be re-processed and ignored instead.",
              pubSubMessageId);
      logger.logWarnWithTracker(dispatcherRequest.isDryRun(), runId, null, msg);

      // end execution successfully and the controller will ACK the request to PubSub and stop
      // retries
      return new PubSubPublishResults(new ArrayList<>(0), new ArrayList<>(0));
    } else {
      logger.logInfoWithTracker(
          runId,
          null,
          String.format("Persisting processing key for PubSub message ID %s", pubSubMessageId));
      persistentSet.add(flagFileName);
    }

    // construct a BigQueryScopeLister using the input resourceScanner implementation
    BigQueryScopeLister bqScopeLister =
        new BigQueryScopeLister(
            resourceScanner,
            new LoggingHelper(
                BigQueryScopeLister.class.getSimpleName(),
                functionNumber,
                config.getProjectId(),
                config.getApplicationName()),
            runId);

    // list of requests to be published to the Tables Dispatcher
    List<JsonMessage> pubSubMessagesToPublish = new ArrayList<>();

    long listingStartTs = System.currentTimeMillis();
    logger.logInfoWithTracker(
        runId, null, "Starting to list down datasets in scope and prepare pubsub messages..");

    // if the tablesIncludeLIst is populated, pass it to the next tables dispatcher directly
    if (dispatcherRequest.getBigQueryScope().getTableIncludeList() != null
        && !dispatcherRequest.getBigQueryScope().getTableIncludeList().isEmpty()) {
      DispatcherTableRequest dispatcherTableRequest =
          new DispatcherTableRequest(
              runId,
              dispatcherRequest.isDryRun(),
              dispatcherRequest.isForceRun(),
              null,
              dispatcherRequest.getBigQueryScope().getTableIncludeList(),
              dispatcherRequest.getBigQueryScope().getTableExcludeList());

      pubSubMessagesToPublish.add(dispatcherTableRequest);
    } else {

      // list all datasets under the scan scope and prepare the Table Dispatcher requests
      List<DatasetSpec> datasetsInScope =
          bqScopeLister.listDatasetsInScope(dispatcherRequest.getBigQueryScope());

      // create one Tables Dispatcher request per in-scope dataset
      for (DatasetSpec datasetSpec : datasetsInScope) {
        pubSubMessagesToPublish.add(
            new DispatcherTableRequest(
                runId,
                dispatcherRequest.isDryRun(),
                dispatcherRequest.isForceRun(),
                datasetSpec,
                dispatcherRequest.getBigQueryScope().getTableIncludeList(),
                dispatcherRequest.getBigQueryScope().getTableExcludeList()));
      }
    }

    long listingEndTs = System.currentTimeMillis();
    logger.logInfoWithTracker(
        runId,
        null,
        String.format(
            "Finished listing down datasets in-scope in %s ms.", listingEndTs - listingStartTs));

    long publishingStartTs = System.currentTimeMillis();
    logger.logInfoWithTracker(
        runId,
        null,
        String.format(
            "Starting to publish to PubSub: %s messages", pubSubMessagesToPublish.size()));

    // Publish the list of requests to PubSub
    PubSubPublishResults publishResults =
        pubSubService.publishTableOperationRequests(
            config.getProjectId(), config.getOutputTopic(), pubSubMessagesToPublish);

    long publishingEndTs = System.currentTimeMillis();
    logger.logInfoWithTracker(
        runId,
        null,
        String.format(
            "Finished publishing to PubSub in %s ms: %s success messages, %s failed messages",
            publishingEndTs - publishingStartTs,
            publishResults.getSuccessMessages().size(),
            publishResults.getFailedMessages().size()));

    // handle failed publishing requests
    for (FailedPubSubMessage msg : publishResults.getFailedMessages()) {
      DispatcherTableRequest request = (DispatcherTableRequest) msg.getMsg();

      String logMsg = String.format("Failed to publish this PubSub messages %s", msg);
      logger.logWarnWithTracker(runId, null, logMsg);

      logger.logFailedDispatcherEntityId(
          runId,
          null,
          // pass the full request because it might be the tablesInclusionList one or the
          // DatasetSpec one
          request.toString(),
          msg.getExceptionMessage(),
          msg.getExceptionClass());
    }

    long dispatcherLogStartTs = System.currentTimeMillis();
    logger.logInfoWithTracker(runId, null, "Starting to list creating dispatched datasets log..");

    // handle success publishing requests
    // TODO: improve and standardize this by writing the log to GCS as external BQ table (like
    // in
    // tables dispatcher)
    for (SuccessPubSubMessage msg : publishResults.getSuccessMessages()) {
      DispatcherTableRequest request = (DispatcherTableRequest) msg.getMsg();

      logger.logSuccessDispatcherDataset(runId, request);
    }

    long dispatcherLogEndTs = System.currentTimeMillis();
    logger.logInfoWithTracker(
        runId,
        null,
        String.format(
            "Finished creating dispatched datasets log in %s ms.", listingEndTs - listingStartTs));

    // log counters for this dispatcher invocation
    logger.logDispatcherCounters(
        runId,
        dispatcherRequest.isDryRun(),
        "datasets-dispatcher",
        dispatcherRequest.getBigQueryScope().toString(),
        pubSubMessagesToPublish.size(),
        dispatcherLogEndTs - functionStartTs,
        listingEndTs - listingStartTs,
        publishingEndTs - publishingStartTs,
        dispatcherLogEndTs - dispatcherLogStartTs);

    logger.logFunctionEnd(runId, null);

    return publishResults;
  }
}
