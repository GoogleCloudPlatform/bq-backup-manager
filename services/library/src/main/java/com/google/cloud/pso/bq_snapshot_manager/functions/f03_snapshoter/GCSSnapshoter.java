/*
 * Copyright 2023 Google LLC
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
import com.google.cloud.pso.bq_snapshot_manager.services.map.PersistentMap;
import com.google.cloud.pso.bq_snapshot_manager.services.pubsub.PubSubService;
import com.google.cloud.pso.bq_snapshot_manager.services.set.PersistentSet;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GCSSnapshoter {

    private final LoggingHelper logger;

    private final SnapshoterConfig config;
    private final BigQueryService bqService;
    private final PubSubService pubSubService;
    private final PersistentSet persistentSet;
    private final String persistentSetObjectPrefix;

    private final PersistentMap persistentMap;

    private final String persistentMapObjectPrefix;

    public GCSSnapshoter(SnapshoterConfig config,
                         BigQueryService bqService,
                         PubSubService pubSubService,
                         PersistentSet persistentSet,
                         String persistentSetObjectPrefix,
                         PersistentMap persistentMap,
                         String persistentMapObjectPrefix,
                         Integer functionNumber
    ) {
        this.config = config;
        this.bqService = bqService;
        this.pubSubService = pubSubService;
        this.persistentSet = persistentSet;
        this.persistentSetObjectPrefix = persistentSetObjectPrefix;
        this.persistentMap = persistentMap;
        this.persistentMapObjectPrefix = persistentMapObjectPrefix;

        logger = new LoggingHelper(
                GCSSnapshoter.class.getSimpleName(),
                functionNumber,
                config.getProjectId()
        );
    }

    public static void validateRequest(SnapshoterRequest request){
        // validate required params
        if (! (request.getBackupPolicy().getMethod().equals(BackupMethod.GCS_SNAPSHOT) ||
                request.getBackupPolicy().getMethod().equals(BackupMethod.BOTH))) {
            throw new IllegalArgumentException(String.format("BackupMethod must be GCS_SNAPSHOT or BOTH. Received %s",
                    request.getBackupPolicy().getMethod()));
        }
        if (request.getBackupPolicy().getGcsExportFormat() == null) {
            throw new IllegalArgumentException(String.format("GCSExportFormat is missing in the BackupPolicy %s",
                    request.getBackupPolicy()));
        }
        if (request.getBackupPolicy().getGcsSnapshotStorageLocation() == null) {
            throw new IllegalArgumentException(String.format("GcsSnapshotStorageLocation is missing in the BackupPolicy %s",
                    request.getBackupPolicy()));
        }
    }

    public GCSSnapshoterResponse execute(SnapshoterRequest request, Timestamp operationTs, String pubSubMessageId) throws IOException, NonRetryableApplicationException, InterruptedException {

        // run common service start logging and checks
        Utils.runServiceStartRoutines(
                logger,
                request,
                persistentSet,
                persistentSetObjectPrefix,
                pubSubMessageId
        );


        // validate required input
        validateRequest(request);

        // Perform the Snapshot operation using the BigQuery service

        // time travel is calculated relative to the operation time
        Tuple<TableSpec, Long> sourceTableWithTimeTravelTuple = Utils.getTableSpecWithTimeTravel(
                request.getTargetTable(),
                request.getBackupPolicy().getTimeTravelOffsetDays(),
                operationTs
        );

        // construct the backup folder for this run in the format project/dataset/table/trackingid/timetravelstamp
        String backupFolder = String.format("%s/%s/%s/%s/%s/%s",
                request.getTargetTable().getProject(),
                request.getTargetTable().getDataset(),
                request.getTargetTable().getTable(),
                request.getTrackingId(),
                sourceTableWithTimeTravelTuple.y(), // include the time travel millisecond for transparency
                request.getBackupPolicy().getGcsExportFormat()
                );

        String gcsDestinationUri = prepareGcsUriForMultiFileExport(
                request.getBackupPolicy().getGcsSnapshotStorageLocation(),
                backupFolder
                );


        Timestamp timeTravelTs = Timestamp.ofTimeSecondsAndNanos(sourceTableWithTimeTravelTuple.y() / 1000, 0);
        logger.logInfoWithTracker(
                request.isDryRun(),
                request.getTrackingId(),
                request.getTargetTable(),
                String.format("Will take a GCS Snapshot for '%s' to '%s' with time travel timestamp '%s' (%s days)",
                        request.getTargetTable().toSqlString(),
                        gcsDestinationUri,
                        timeTravelTs,
                        request.getBackupPolicy().getTimeTravelOffsetDays().getText()
                )
        );

        if(!request.isDryRun()){
            // create an async bq export job

            String jobId = TrackingHelper.generateBQExportJobId(request.getTrackingId());

            // We create the tagging request and added it to a persistent storage
            // The Tagger service will receive notifications of export job completion via log sinks and pick up the tagger request from the persistent storage
            // Make sure the file is stored first before running the export job. In case of non-fatal error of file creation and retry, we don't re-run the export job
            TaggerRequest taggerRequest = new TaggerRequest(
                    request.getTargetTable(),
                    request.getRunId(),
                    request.getTrackingId(),
                    request.isDryRun(),
                    request.getBackupPolicy(),
                    BackupMethod.GCS_SNAPSHOT,
                    null,
                    gcsDestinationUri,
                    operationTs
            );

            String taggerRequestFile = String.format("%s/%s", persistentMapObjectPrefix, jobId);
            persistentMap.put(taggerRequestFile, taggerRequest.toJsonString());

            Map<String, String> jobLabels = new HashMap<>();
            // labels has to be max 63 chars, contain only lowercase letters, numeric characters, underscores, and dashes. All characters must use UTF-8 encoding, and international characters are allowed.
            jobLabels.put("app", Globals.APPLICATION_NAME);

            // API Call
            bqService.exportToGCS(
                    jobId,
                    sourceTableWithTimeTravelTuple.x(),
                    gcsDestinationUri,
                    request.getBackupPolicy().getGcsExportFormat(),
                    request.getBackupPolicy().getGcsCsvDelimiter(),
                    request.getBackupPolicy().getGcsCsvExportHeader(),
                    request.getBackupPolicy().getGcsUseAvroLogicalTypes(),
                    request.getTrackingId(),
                    jobLabels
            );
        }

        logger.logInfoWithTracker(
                request.isDryRun(),
                request.getTrackingId(),
                request.getTargetTable(),
                String.format("BigQuery GCS export submitted for table %s to %s",
                        request.getTargetTable().toSqlString(),
                        gcsDestinationUri
                )
        );

        // run common service end logging and adding pubsub message to processed list
        Utils.runServiceEndRoutines(
                logger,
                request,
                persistentSet,
                persistentSetObjectPrefix,
                pubSubMessageId
        );

        return new GCSSnapshoterResponse(
                request.getTargetTable(),
                request.getRunId(),
                request.getTrackingId(),
                request.isDryRun(),
                operationTs,
                sourceTableWithTimeTravelTuple.x()
        );
    }


    public static String prepareGcsUriForMultiFileExport(String gcsUri, String folderName) {
        // when exporting multiple files the uri should be gs://path/*
        String cleanUri = Utils.trimSlashes(gcsUri);
        String cleanFolder = Utils.trimSlashes(folderName);

        return String.format("%s/%s/*", cleanUri, cleanFolder);
    }


}
