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
package com.google.cloud.pso.bq_snapshot_manager.functions.f02_configurator;

import com.google.cloud.Timestamp;
import com.google.cloud.Tuple;
import com.google.cloud.pso.bq_snapshot_manager.entities.JsonMessage;
import com.google.cloud.pso.bq_snapshot_manager.entities.TableSpec;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.BackupConfigSource;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.BackupMethod;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.BackupPolicy;
import com.google.cloud.pso.bq_snapshot_manager.entities.NonRetryableApplicationException;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.FallbackBackupPolicy;
import com.google.cloud.pso.bq_snapshot_manager.functions.f03_snapshoter.SnapshoterRequest;
import com.google.cloud.pso.bq_snapshot_manager.helpers.LoggingHelper;
import com.google.cloud.pso.bq_snapshot_manager.helpers.TrackingHelper;
import com.google.cloud.pso.bq_snapshot_manager.helpers.Utils;
import com.google.cloud.pso.bq_snapshot_manager.services.catalog.DataCatalogService;
import com.google.cloud.pso.bq_snapshot_manager.services.pubsub.PubSubPublishResults;
import com.google.cloud.pso.bq_snapshot_manager.services.pubsub.PubSubService;
import com.google.cloud.pso.bq_snapshot_manager.services.set.PersistentSet;
import org.springframework.scheduling.support.CronExpression;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

public class Configurator {

    private final LoggingHelper logger;
    private final Integer functionNumber;
    private final ConfiguratorConfig config;
    private final DataCatalogService dataCatalogService;
    private final PubSubService pubSubService;
    private final PersistentSet persistentSet;
    private final FallbackBackupPolicy fallbackBackupPolicy;
    private final String persistentSetObjectPrefix;


    public Configurator(ConfiguratorConfig config,
                        DataCatalogService dataCatalogService,
                        PubSubService pubSubService,
                        PersistentSet persistentSet,
                        FallbackBackupPolicy fallbackBackupPolicy,
                        String persistentSetObjectPrefix,
                        Integer functionNumber) {
        this.config = config;
        this.dataCatalogService = dataCatalogService;
        this.pubSubService = pubSubService;
        this.persistentSet = persistentSet;
        this.fallbackBackupPolicy = fallbackBackupPolicy;
        this.persistentSetObjectPrefix = persistentSetObjectPrefix;
        this.functionNumber = functionNumber;

        logger = new LoggingHelper(
                Configurator.class.getSimpleName(),
                functionNumber,
                config.getProjectId()
        );
    }

    public ConfiguratorResponse execute(ConfiguratorRequest request, String pubSubMessageId) throws IOException, NonRetryableApplicationException, InterruptedException {

        // run common service start logging and checks
        Utils.runServiceStartRoutines(
                logger,
                request,
                persistentSet,
                persistentSetObjectPrefix,
                pubSubMessageId
        );

        // 1. Find the backup policy of this table
        BackupPolicy backupPolicy = getBackupPolicy(request);

        // 2. Determine if we should take a backup at this run given the policy CRON expression
        // if the table has been backed up before then check if we should backup at this run

        // use the start time of this run as a reference point in time for CRON checks across all requests in this run
        Timestamp refTs = TrackingHelper.parseRunIdAsTimestamp(request.getRunId());
        boolean isBackupTime = isBackupTime(
                request.isForceRun(),
                request.getTargetTable(),
                backupPolicy.getCron(),
                refTs,
                backupPolicy.getConfigSource(),
                backupPolicy.getLastBackupAt(),
                logger,
                request.getTrackingId()
        );

        // 3. Prepare and send the backup request(s) if required
        SnapshoterRequest bqSnapshotRequest = null;
        SnapshoterRequest gcsSnapshotRequest = null;
        PubSubPublishResults bqSnapshotPublishResults = null;
        PubSubPublishResults gcsSnapshotPublishResults = null;
        if (isBackupTime) {
            Tuple<SnapshoterRequest, SnapshoterRequest> snapshotRequestsTuple = prepareSnapshotRequests(
                    backupPolicy,
                    request
            );

            List<JsonMessage> bqSnapshotRequests = new ArrayList<>(1);
            if (snapshotRequestsTuple.x() != null) {
                bqSnapshotRequest = snapshotRequestsTuple.x();
                bqSnapshotRequests.add(bqSnapshotRequest);
            }

            List<JsonMessage> gcsSnapshotRequests = new ArrayList<>(1);
            if (snapshotRequestsTuple.y() != null) {
                gcsSnapshotRequest = snapshotRequestsTuple.y();
                gcsSnapshotRequests.add(gcsSnapshotRequest);
            }

            // Publish the list of bq snapshot requests to PubSub
            bqSnapshotPublishResults = pubSubService.publishTableOperationRequests(
                    config.getProjectId(),
                    config.getBigQuerySnapshoterTopic(),
                    bqSnapshotRequests
            );

            // Publish the list of gcs snapshot requests to PubSub
            gcsSnapshotPublishResults = pubSubService.publishTableOperationRequests(
                    config.getProjectId(),
                    config.getGcsSnapshoterTopic(),
                    gcsSnapshotRequests
            );

            if (!bqSnapshotPublishResults.getSuccessMessages().isEmpty()) {
                logger.logInfoWithTracker(
                        request.isDryRun(),
                        request.getTrackingId(),
                        request.getTargetTable(),
                        String.format("Published %s BigQuery Snapshot requests %s",
                                bqSnapshotPublishResults.getSuccessMessages().size(),
                                bqSnapshotPublishResults.getSuccessMessages())
                );
            }

            if (!gcsSnapshotPublishResults.getSuccessMessages().isEmpty()) {
                logger.logInfoWithTracker(
                        request.isDryRun(),
                        request.getTrackingId(),
                        request.getTargetTable(),
                        String.format("Published %s GCS Snapshot requests %s",
                                gcsSnapshotPublishResults.getSuccessMessages().size(),
                                gcsSnapshotPublishResults.getSuccessMessages())
                );
            }

            if (!bqSnapshotPublishResults.getFailedMessages().isEmpty()) {
                logger.logWarnWithTracker(
                        request.isDryRun(),
                        request.getTrackingId(),
                        request.getTargetTable(),
                        String.format("Failed to publish BigQuery Snapshot request %s", bqSnapshotPublishResults.getFailedMessages().toString())

                );
            }

            if (!gcsSnapshotPublishResults.getFailedMessages().isEmpty()) {
                logger.logWarnWithTracker(
                        request.isDryRun(),
                        request.getTrackingId(),
                        request.getTargetTable(),
                        String.format("Failed to publish GCS Snapshot request %s", gcsSnapshotPublishResults.getFailedMessages().toString())

                );
            }
        }

        // run common service end logging and adding pubsub message to processed list
        Utils.runServiceEndRoutines(
                logger,
                request,
                persistentSet,
                persistentSetObjectPrefix,
                pubSubMessageId
        );

        return new ConfiguratorResponse(
                request.getTargetTable(),
                request.getRunId(),
                request.getTrackingId(),
                request.isDryRun(),
                backupPolicy,
                refTs,
                isBackupTime,
                bqSnapshotRequest,
                gcsSnapshotRequest,
                bqSnapshotPublishResults,
                gcsSnapshotPublishResults
                );
    }

    public BackupPolicy getBackupPolicy(ConfiguratorRequest request) throws IOException {

        // Check if the table has a back policy attached to it in data catalog
        BackupPolicy attachedBackupPolicy = dataCatalogService.getBackupPolicyTag(
                request.getTargetTable(),
                config.getBackupTagTemplateId()
        );

        // if there is manually attached backup policy (e.g. by the table designer) then use it.
        if (attachedBackupPolicy != null && attachedBackupPolicy.getConfigSource().equals(BackupConfigSource.MANUAL)) {
            return attachedBackupPolicy;
        }else{

            // find the most granular fallback policy table > dataset > project
            BackupPolicy fallbackPolicy = findFallbackBackupPolicy(fallbackBackupPolicy,
                    request.getTargetTable()).y();

            // if there is a system attached policy, then only use the last_xyz fields from it and use the latest fallback policy
            // the last_backup_at needs to be checked to determine if we should take a backup in this run
            // the last_xyz_uri fields need to be propagated to the Tagger service so that they are not lost on each run
            if (attachedBackupPolicy != null && attachedBackupPolicy.getConfigSource().equals(BackupConfigSource.SYSTEM)) {
                return BackupPolicy.BackupPolicyBuilder
                        .from(fallbackPolicy)
                        .setLastBackupAt(attachedBackupPolicy.getLastBackupAt())
                        .setLastGcsSnapshotStorageUri(attachedBackupPolicy.getLastGcsSnapshotStorageUri())
                        .setLastBqSnapshotStorageUri(attachedBackupPolicy.getLastBqSnapshotStorageUri())
                        .build();
            }else{
                // if there is no attached policy, use fallback one
                return fallbackPolicy;
            }
        }
    }

    public static boolean isBackupTime(
            boolean isForceRun,
            TableSpec targetTable,
            String cron,
            Timestamp referencePoint,
            BackupConfigSource configSource,
            Timestamp lastBackupAt,
            LoggingHelper logger,
            String trackingId
    ) {

        boolean takeBackup;

        if (isForceRun
                || (configSource.equals(BackupConfigSource.SYSTEM)
                && lastBackupAt == null)) {
            // this means the table has not been backed up before
            // CASE 1: It's a force run --> take backup
            // CASE 2: It's a fallback configuration (SYSTEM) running for the first time --> take backup
            takeBackup = true;
        } else {

            if (lastBackupAt == null) {
                // this means the table has not been backed up before
                // CASE 3: It's a MANUAL config but running for the first time
                takeBackup = true;
            } else {

                // CASE 4: It's MANUAL OR SYSTEM config that ran before and already attached to the table
                // --> decide based on CRON next trigger check

                Tuple<Boolean, LocalDateTime> takeBackupTuple = getCronNextTrigger(
                        cron,
                        lastBackupAt,
                        referencePoint
                );

                // .x() is a boolean flag to take a backup or not
                // .y() is the calculated next cron date used in the comparison
                if (takeBackupTuple.x()) {
                    takeBackup = true;
                    logger.logInfoWithTracker(
                            trackingId,
                            targetTable,
                            String.format("Will backup table %s at this run. Calculated next backup time is %s and this run is %s",
                                    targetTable.toSqlString(), takeBackupTuple.y(), referencePoint));

                } else {
                    takeBackup = false;
                    logger.logInfoWithTracker(
                            trackingId,
                            targetTable,
                            String.format("Will skip backup for table %s at this run. Calculated next backup time is %s and this run is %s",
                                    targetTable.toSqlString(), takeBackupTuple.y(), referencePoint));
                }
            }
        }
        return takeBackup;
    }

    /**
     * Checks if a cron expression next trigger time has already passed given a reference point in time (e.g. now)
     *
     * @param cronExpression
     * @param lastBackupAtTs
     * @param referencePoint
     * @return Tuple of yes/no and computed next cron expression
     */
    public static Tuple<Boolean, LocalDateTime> getCronNextTrigger(String cronExpression,
                                                                   Timestamp lastBackupAtTs,
                                                                   Timestamp referencePoint
    ) {

        CronExpression cron = CronExpression.parse(cronExpression);

        LocalDateTime nowDt = LocalDateTime.ofEpochSecond(
                referencePoint.getSeconds(),
                0,
                ZoneOffset.UTC
        );

        LocalDateTime lastBackupAtDt = LocalDateTime.ofEpochSecond(
                lastBackupAtTs.getSeconds(),
                0,
                ZoneOffset.UTC
        );

        // get next execution date based on the last backup date
        LocalDateTime nextExecutionDt = cron.next(lastBackupAtDt);

        return Tuple.of(
                nextExecutionDt.isBefore(nowDt),
                nextExecutionDt
        );
    }

    public Tuple<SnapshoterRequest, SnapshoterRequest> prepareSnapshotRequests(BackupPolicy backupPolicy, ConfiguratorRequest request) {

        SnapshoterRequest bqSnapshotRequest = null;
        SnapshoterRequest gcsSnapshotRequest = null;

        if (backupPolicy.getMethod().equals(BackupMethod.BIGQUERY_SNAPSHOT) || backupPolicy.getMethod().equals(BackupMethod.BOTH)) {
            bqSnapshotRequest = new SnapshoterRequest(
                    request.getTargetTable(),
                    request.getRunId(),
                    request.getTrackingId(),
                    request.isDryRun(),
                    backupPolicy
            );
        }

        if (backupPolicy.getMethod().equals(BackupMethod.GCS_SNAPSHOT) || backupPolicy.getMethod().equals(BackupMethod.BOTH)) {
            gcsSnapshotRequest = new SnapshoterRequest(
                    request.getTargetTable(),
                    request.getRunId(),
                    request.getTrackingId(),
                    request.isDryRun(),
                    backupPolicy
            );
        }

        return Tuple.of(bqSnapshotRequest, gcsSnapshotRequest);
    }

    public static Tuple<String, BackupPolicy> findFallbackBackupPolicy(FallbackBackupPolicy
                                                                               fallbackBackupPolicy,
                                                                       TableSpec tableSpec) {

        BackupPolicy tableLevel = fallbackBackupPolicy.getTableOverrides().get(tableSpec.toSqlString());
        if (tableLevel != null) {
            return Tuple.of("table", tableLevel);
        }

        BackupPolicy datasetLevel = fallbackBackupPolicy.getDatasetOverrides().get(
                String.format("%s.%s", tableSpec.getProject(), tableSpec.getDataset())
        );
        if (datasetLevel != null) {
            return Tuple.of("dataset", datasetLevel);
        }

        BackupPolicy projectLevel = fallbackBackupPolicy.getProjectOverrides().get(
                tableSpec.getProject()
        );
        if (projectLevel != null) {
            return Tuple.of("project", projectLevel);
        }

        //TODO: check for folder level by getting the folder id of a project from the API

        // else return the global default policy
        return Tuple.of("global", fallbackBackupPolicy.getDefaultPolicy());
    }
}
