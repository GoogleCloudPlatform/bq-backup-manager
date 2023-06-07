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

package com.google.cloud.pso.bq_snapshot_manager.functions.f03_snapshoter;


import com.google.cloud.Timestamp;
import com.google.cloud.Tuple;
import com.google.cloud.pso.bq_snapshot_manager.entities.Globals;
import com.google.cloud.pso.bq_snapshot_manager.entities.NonRetryableApplicationException;
import com.google.cloud.pso.bq_snapshot_manager.entities.TableSpec;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.BackupMethod;
import com.google.cloud.pso.bq_snapshot_manager.functions.f04_tagger.TaggerRequest;
import com.google.cloud.pso.bq_snapshot_manager.helpers.LoggingHelper;
import com.google.cloud.pso.bq_snapshot_manager.helpers.TrackingHelper;
import com.google.cloud.pso.bq_snapshot_manager.helpers.Utils;
import com.google.cloud.pso.bq_snapshot_manager.services.bq.BigQueryService;
import com.google.cloud.pso.bq_snapshot_manager.services.pubsub.FailedPubSubMessage;
import com.google.cloud.pso.bq_snapshot_manager.services.pubsub.PubSubPublishResults;
import com.google.cloud.pso.bq_snapshot_manager.services.pubsub.PubSubService;
import com.google.cloud.pso.bq_snapshot_manager.services.pubsub.SuccessPubSubMessage;
import com.google.cloud.pso.bq_snapshot_manager.services.set.PersistentSet;

import java.io.IOException;
import java.util.Arrays;

public class BigQuerySnapshoter {

    private final LoggingHelper logger;

    private final SnapshoterConfig config;
    private final BigQueryService bqService;
    private final PubSubService pubSubService;
    private final PersistentSet persistentSet;
    private final String persistentSetObjectPrefix;


    public BigQuerySnapshoter(SnapshoterConfig config,
                              BigQueryService bqService,
                              PubSubService pubSubService,
                              PersistentSet persistentSet,
                              String persistentSetObjectPrefix,
                              Integer functionNumber
    ) {
        this.config = config;
        this.bqService = bqService;
        this.pubSubService = pubSubService;
        this.persistentSet = persistentSet;
        this.persistentSetObjectPrefix = persistentSetObjectPrefix;

        logger = new LoggingHelper(
                BigQuerySnapshoter.class.getSimpleName(),
                functionNumber,
                config.getProjectId()
        );
    }

    public static void validateInput(SnapshoterRequest request){
        if (! (request.getBackupPolicy().getMethod().equals(BackupMethod.BIGQUERY_SNAPSHOT) ||
                request.getBackupPolicy().getMethod().equals(BackupMethod.BOTH))) {
            throw new IllegalArgumentException(String.format("BackupMethod must be BIGQUERY_SNAPSHOT or BOTH. Received %s",
                    request.getBackupPolicy().getMethod()));
        }
        if (request.getBackupPolicy().getBigQuerySnapshotExpirationDays() == null) {
            throw new IllegalArgumentException(String.format("BigQuerySnapshotExpirationDays is missing in the BackupPolicy %s",
                    request.getBackupPolicy()));
        }
        if (request.getBackupPolicy().getBackupProject() == null) {
            throw new IllegalArgumentException(String.format("BigQuerySnapshotStorageProject is missing in the BackupPolicy %s",
                    request.getBackupPolicy()));
        }
        if (request.getBackupPolicy().getBigQuerySnapshotStorageDataset() == null) {
            throw new IllegalArgumentException(String.format("BigQuerySnapshotStorageDataset is missing in the BackupPolicy %s",
                    request.getBackupPolicy()));
        }
    }

    public BigQuerySnapshoterResponse execute(SnapshoterRequest request, Timestamp operationTs, String pubSubMessageId) throws IOException, NonRetryableApplicationException, InterruptedException {

        // run common service start logging and checks
        Utils.runServiceStartRoutines(
                logger,
                request,
                persistentSet,
                persistentSetObjectPrefix,
                pubSubMessageId
        );

        // validate required params
        validateInput(request);

        // Perform the Snapshot operation using the BigQuery service

        // expiry date is calculated relative to the operation time
        Timestamp expiryTs = Timestamp.ofTimeSecondsAndNanos(
                operationTs.getSeconds() + (request.getBackupPolicy().getBigQuerySnapshotExpirationDays().longValue() * 86400L),
                0);

        // time travel is calculated relative to the operation time
        Tuple<TableSpec, Long> sourceTableWithTimeTravelTuple = Utils.getTableSpecWithTimeTravel(
                request.getTargetTable(),
                request.getBackupPolicy().getTimeTravelOffsetDays(),
                operationTs
        );

        // construct the snapshot table from the request params and calculated timetravel
        TableSpec snapshotTable = getSnapshotTableSpec(
                request.getTargetTable(),
                request.getBackupPolicy().getBackupProject(),
                request.getBackupPolicy().getBigQuerySnapshotStorageDataset(),
                request.getRunId(),
                sourceTableWithTimeTravelTuple.y()
        );

        Timestamp timeTravelTs = Timestamp.ofTimeSecondsAndNanos(sourceTableWithTimeTravelTuple.y()/1000, 0);
        logger.logInfoWithTracker(
                request.isDryRun(),
                request.getTrackingId(),
                request.getTargetTable(),
                String.format("Will take a BQ Snapshot for '%s' to '%s' with time travel timestamp '%s' (%s days) expiring on '%s'",
                        request.getTargetTable().toSqlString(),
                        snapshotTable.toSqlString(),
                        timeTravelTs.toString(),
                        request.getBackupPolicy().getTimeTravelOffsetDays().getText(),
                        expiryTs.toString()
                )
        );


        if(!request.isDryRun()){
            // API Call
            String jobId = TrackingHelper.generateBQSnapshotJobId(request.getTrackingId());

            bqService.createSnapshot(
                    jobId,
                    sourceTableWithTimeTravelTuple.x(),
                    snapshotTable,
                    expiryTs,
                    request.getTrackingId()
            );
        }

        logger.logInfoWithTracker(
                request.isDryRun(),
                request.getTrackingId(),
                request.getTargetTable(),
                String.format("BigQuery snapshot completed for table %s to %s",
                        request.getTargetTable().toSqlString(),
                        snapshotTable.toSqlString()
                )
        );

        // Create a Tagger request and send it to the Tagger PubSub topic
        TaggerRequest taggerRequest = new TaggerRequest(
                request.getTargetTable(),
                request.getRunId(),
                request.getTrackingId(),
                request.isDryRun(),
                request.getBackupPolicy(),
                BackupMethod.BIGQUERY_SNAPSHOT,
                snapshotTable,
                null,
                operationTs
        );

        // Publish the list of tagging requests to PubSub
        PubSubPublishResults publishResults = pubSubService.publishTableOperationRequests(
                config.getProjectId(),
                config.getOutputTopic(),
                Arrays.asList(taggerRequest)
        );

        for (FailedPubSubMessage msg : publishResults.getFailedMessages()) {
            String logMsg = String.format("Failed to publish this message %s", msg.toString());
            logger.logWarnWithTracker(request.isDryRun(),request.getTrackingId(), request.getTargetTable(), logMsg);
        }

        for (SuccessPubSubMessage msg : publishResults.getSuccessMessages()) {
            String logMsg = String.format("Published this message %s", msg.toString());
            logger.logInfoWithTracker(request.isDryRun(),request.getTrackingId(), request.getTargetTable(), logMsg);
        }

        // run common service end logging and adding pubsub message to processed list
        Utils.runServiceEndRoutines(
                logger,
                request,
                persistentSet,
                persistentSetObjectPrefix,
                pubSubMessageId
        );

        return new BigQuerySnapshoterResponse(
                request.getTargetTable(),
                request.getRunId(),
                request.getTrackingId(),
                request.isDryRun(),
                operationTs,
                sourceTableWithTimeTravelTuple.x(),
                snapshotTable,
                taggerRequest,
                publishResults
        );
    }

    public static TableSpec getSnapshotTableSpec(TableSpec sourceTable, String snapshotProject, String snapshotDataset, String runId, Long timeTravelMs){
        return new TableSpec(
                snapshotProject,
                snapshotDataset,
                // Construct a snapshot table name that
                // 1. doesn't collide with other snapshots from other datasets, projects or runs
                // 2. propagates the time travel used to take this snapshot (we don't use labels to avoid extra API calls)
                String.format("%s_%s_%s_%s_%s",
                        sourceTable.getProject(),
                        sourceTable.getDataset(),
                        sourceTable.getTable(),
                        runId,
                        timeTravelMs
                )
        );
    }
}
