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

package com.google.cloud.pso.bq_snapshot_manager.functions.f02_configurator;


import com.google.cloud.Timestamp;
import com.google.cloud.Tuple;
import com.google.cloud.pso.bq_snapshot_manager.entities.JsonMessage;
import com.google.cloud.pso.bq_snapshot_manager.entities.NonRetryableApplicationException;
import com.google.cloud.pso.bq_snapshot_manager.entities.TableSpec;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.*;
import com.google.cloud.pso.bq_snapshot_manager.functions.f03_snapshoter.SnapshoterRequest;
import com.google.cloud.pso.bq_snapshot_manager.helpers.LoggingHelper;
import com.google.cloud.pso.bq_snapshot_manager.helpers.Utils;
import com.google.cloud.pso.bq_snapshot_manager.services.backup_policy.BackupPolicyService;
import com.google.cloud.pso.bq_snapshot_manager.services.bq.BigQueryService;
import com.google.cloud.pso.bq_snapshot_manager.services.pubsub.PubSubPublishResults;
import com.google.cloud.pso.bq_snapshot_manager.services.pubsub.PubSubService;
import com.google.cloud.pso.bq_snapshot_manager.services.scan.ResourceScanner;
import com.google.cloud.pso.bq_snapshot_manager.services.set.PersistentSet;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.springframework.scheduling.support.CronExpression;

public class Configurator {

    private final LoggingHelper logger;
    private final Integer functionNumber;
    private final ConfiguratorConfig config;

    private final BigQueryService bqService;

    private final BackupPolicyService backupPolicyService;
    private final PubSubService pubSubService;

    private final ResourceScanner resourceScanner;
    private final PersistentSet persistentSet;
    private final FallbackBackupPolicy fallbackBackupPolicy;
    private final String persistentSetObjectPrefix;

    public Configurator(
            ConfiguratorConfig config,
            BigQueryService bqService,
            BackupPolicyService backupPolicyService,
            PubSubService pubSubService,
            ResourceScanner resourceScanner,
            PersistentSet persistentSet,
            FallbackBackupPolicy fallbackBackupPolicy,
            String persistentSetObjectPrefix,
            Integer functionNumber) {
        this.config = config;
        this.bqService = bqService;
        this.backupPolicyService = backupPolicyService;
        this.pubSubService = pubSubService;
        this.resourceScanner = resourceScanner;
        this.persistentSet = persistentSet;
        this.fallbackBackupPolicy = fallbackBackupPolicy;
        this.persistentSetObjectPrefix = persistentSetObjectPrefix;
        this.functionNumber = functionNumber;

        logger =
                new LoggingHelper(
                        Configurator.class.getSimpleName(),
                        functionNumber,
                        config.getProjectId(),
                        config.getApplicationName());
    }

    public ConfiguratorResponse execute(ConfiguratorRequest request, String pubSubMessageId)
            throws IOException, NonRetryableApplicationException, InterruptedException {

        // run common service start logging and checks
        Utils.runServiceStartRoutines(
                logger, request, persistentSet, persistentSetObjectPrefix, request.getTrackingId());

        // 1. Find the backup policy of this table
        Tuple<BackupPolicyAndState, String> backupPolicyTuple = getBackupPolicyAndState(request);
        BackupPolicyAndState backupPolicy = backupPolicyTuple.x();

        // 2a. Determine if we should take a backup at this run given the policy CRON expression
        // if the table has been backed up before then check if we should backup at this run
        boolean isBackupCronTime =
                isBackupCronTime(
                        request.getTargetTable(),
                        backupPolicy.getCron(),
                        request.getRefTimestamp(),
                        backupPolicy.getConfigSource(),
                        backupPolicy.getLastBackupAt(),
                        logger,
                        request.getTrackingId());

        // 2b. Check if the table has been created before the desired time travel
        Tuple<TableSpec, Long> sourceTableWithTimeTravelTuple =
                Utils.getTableSpecWithTimeTravel(
                        request.getTargetTable(),
                        backupPolicy.getTimeTravelOffsetDays(),
                        request.getRefTimestamp());

        // API Call
        Long tableCreationTime = bqService.getTableCreationTime(request.getTargetTable());

        // check if the table is created after the time travel timestamp
        boolean isTableCreatedBeforeTimeTravel =
                tableCreationTime < sourceTableWithTimeTravelTuple.y();

        // To take a backup, the backup cron expression must match this run and the table has to be
        // around for
        // enough time to apply the desired time travel. Otherwise, skip the backup this run
        boolean isBackupTime =
                isBackupTime(
                        request.isForceRun(), isBackupCronTime, isTableCreatedBeforeTimeTravel);

        logger.logInfoWithTracker(
                request.isDryRun(),
                request.getTrackingId(),
                request.getTargetTable(),
                String.format(
                        "isBackupTime for this run is '%s'. Calculated based on isForceRun=%s, isBackupCronTime=%s, isTableCreatedBeforeTimeTravel=%s ",
                        isBackupTime,
                        request.isForceRun(),
                        isBackupCronTime,
                        isTableCreatedBeforeTimeTravel));

        // 3. Prepare and send the backup request(s) if required
        SnapshoterRequest bqSnapshotRequest = null;
        SnapshoterRequest gcsSnapshotRequest = null;
        PubSubPublishResults bqSnapshotPublishResults = null;
        PubSubPublishResults gcsSnapshotPublishResults = null;
        if (isBackupTime) {
            Tuple<SnapshoterRequest, SnapshoterRequest> snapshotRequestsTuple =
                    prepareSnapshotRequests(backupPolicy, request);

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
            bqSnapshotPublishResults =
                    pubSubService.publishTableOperationRequests(
                            config.getProjectId(),
                            config.getBigQuerySnapshoterTopic(),
                            bqSnapshotRequests);

            // Publish the list of gcs snapshot requests to PubSub
            gcsSnapshotPublishResults =
                    pubSubService.publishTableOperationRequests(
                            config.getProjectId(),
                            config.getGcsSnapshoterTopic(),
                            gcsSnapshotRequests);

            if (!bqSnapshotPublishResults.getSuccessMessages().isEmpty()) {
                logger.logInfoWithTracker(
                        request.isDryRun(),
                        request.getTrackingId(),
                        request.getTargetTable(),
                        String.format(
                                "Published %s BigQuery Snapshot requests %s",
                                bqSnapshotPublishResults.getSuccessMessages().size(),
                                bqSnapshotPublishResults.getSuccessMessages()));
            }

            if (!gcsSnapshotPublishResults.getSuccessMessages().isEmpty()) {
                logger.logInfoWithTracker(
                        request.isDryRun(),
                        request.getTrackingId(),
                        request.getTargetTable(),
                        String.format(
                                "Published %s GCS Snapshot requests %s",
                                gcsSnapshotPublishResults.getSuccessMessages().size(),
                                gcsSnapshotPublishResults.getSuccessMessages()));
            }

            if (!bqSnapshotPublishResults.getFailedMessages().isEmpty()) {
                logger.logWarnWithTracker(
                        request.isDryRun(),
                        request.getTrackingId(),
                        request.getTargetTable(),
                        String.format(
                                "Failed to publish BigQuery Snapshot request %s",
                                bqSnapshotPublishResults.getFailedMessages().toString()));
            }

            if (!gcsSnapshotPublishResults.getFailedMessages().isEmpty()) {
                logger.logWarnWithTracker(
                        request.isDryRun(),
                        request.getTrackingId(),
                        request.getTargetTable(),
                        String.format(
                                "Failed to publish GCS Snapshot request %s",
                                gcsSnapshotPublishResults.getFailedMessages().toString()));
            }
        }

        // run common service end logging and adding pubsub message to processed list
        Utils.runServiceEndRoutines(
                logger, request, persistentSet, persistentSetObjectPrefix, request.getTrackingId());

        return new ConfiguratorResponse(
                request.getTargetTable(),
                request.getRunId(),
                request.getTrackingId(),
                request.isDryRun(),
                backupPolicy,
                backupPolicyTuple.y(), // source of the backup policy
                request.getRefTimestamp(),
                isBackupCronTime,
                isTableCreatedBeforeTimeTravel,
                isBackupTime,
                bqSnapshotRequest,
                gcsSnapshotRequest,
                bqSnapshotPublishResults,
                gcsSnapshotPublishResults);
    }

    /**
     * Retrieve the backup policy and state for a single table either from the table-level policy
     * store or from fallback policies
     *
     * @param request
     * @return Tuple<BackupPolicy, String> where x = the backup policy for the table and y =
     *     description of how the backup policy was found/computed (used for logging and debugging)
     * @throws IOException
     */
    public Tuple<BackupPolicyAndState, String> getBackupPolicyAndState(ConfiguratorRequest request)
            throws IOException {

        // Check if the table has a back policy attached to it
        BackupPolicyAndState attachedBackupPolicyAndState =
                backupPolicyService.getBackupPolicyAndStateForTable(request.getTargetTable());

        // if there is manually attached backup policy (e.g. by the table designer) then use it.
        if (attachedBackupPolicyAndState != null
                && attachedBackupPolicyAndState
                        .getConfigSource()
                        .equals(BackupConfigSource.MANUAL)) {

            logger.logInfoWithTracker(
                    request.isDryRun(),
                    request.getTrackingId(),
                    request.getTargetTable(),
                    String.format(
                            "Attached backup policy found for table %s", request.getTargetTable()));

            return Tuple.of(
                    attachedBackupPolicyAndState,
                    attachedBackupPolicyAndState.getState() != null
                            ? "Manually attached policy with backup state from previous runs"
                            : "Manually attached policy without backup state from previous runs");
        } else {

            logger.logInfoWithTracker(
                    request.isDryRun(),
                    request.getTrackingId(),
                    request.getTargetTable(),
                    String.format(
                            "No 'config_source=MANUAL' backup policy found for table %s. Will search for a fallback policy.",
                            request.getTargetTable()));

            // find the most granular fallback policy table > dataset > project
            Tuple<String, BackupPolicy> fallbackBackupPolicyTuple =
                    findFallbackBackupPolicy(
                            fallbackBackupPolicy, request.getTargetTable(), request.getRunId());

            BackupPolicy fallbackPolicy = fallbackBackupPolicyTuple.y();

            logger.logInfoWithTracker(
                    request.isDryRun(),
                    request.getTrackingId(),
                    request.getTargetTable(),
                    String.format(
                            "Will use a %s-level fallback policy", fallbackBackupPolicyTuple.x()));

            // if there is a system attached policy, then only use the last_xyz fields from it and
            // use the
            // latest fallback policy
            // the last_backup_at needs to be checked to determine if we should take a backup in
            // this run
            // the last_xyz_uri fields need to be propagated to the Tagger service so that they are
            // not
            // lost on each run
            if (attachedBackupPolicyAndState != null
                    && attachedBackupPolicyAndState
                            .getConfigSource()
                            .equals(BackupConfigSource.SYSTEM)) {
                return Tuple.of(
                        new BackupPolicyAndState(
                                fallbackPolicy,
                                new BackupState(
                                        attachedBackupPolicyAndState.getLastBackupAt(),
                                        attachedBackupPolicyAndState.getLastBqSnapshotStorageUri(),
                                        attachedBackupPolicyAndState
                                                .getLastGcsSnapshotStorageUri())),
                        String.format(
                                "System attached fallback policy on level '%s' with backup state from previous runs",
                                fallbackBackupPolicyTuple.x()));
            } else {
                // if there is no attached policy, use fallback one
                return Tuple.of(
                        new BackupPolicyAndState(fallbackPolicy, null),
                        String.format(
                                "System attached fallback policy on level '%s' without backup state from previous runs",
                                fallbackBackupPolicyTuple.x()));
            }
        }
    }

    public static boolean isBackupCronTime(
            TableSpec targetTable,
            String cron,
            Timestamp referencePoint,
            BackupConfigSource configSource,
            Timestamp lastBackupAt,
            LoggingHelper logger,
            String trackingId) {

        boolean takeBackup;

        if (configSource.equals(BackupConfigSource.SYSTEM) && lastBackupAt == null) {
            // this means the table has not been backed up before
            // CASE 1: It's a fallback configuration (SYSTEM) running for the first time --> take
            // backup
            takeBackup = true;
        } else {

            if (lastBackupAt == null) {
                // this means the table has not been backed up before
                // CASE 2: It's a MANUAL config but running for the first time
                takeBackup = true;
            } else {

                // CASE 3: It's MANUAL OR SYSTEM config that ran before and already attached to the
                // table
                // --> decide based on CRON next trigger check

                Tuple<Boolean, LocalDateTime> takeBackupTuple =
                        getCronNextTrigger(cron, lastBackupAt, referencePoint);

                // .x() is a boolean flag to take a backup or not
                // .y() is the calculated next cron date used in the comparison
                if (takeBackupTuple.x()) {
                    takeBackup = true;
                    logger.logInfoWithTracker(
                            trackingId,
                            targetTable,
                            String.format(
                                    "Will backup table %s at this run. Calculated next backup time is %s and this run is %s",
                                    targetTable.toSqlString(),
                                    takeBackupTuple.y(),
                                    referencePoint));

                } else {
                    takeBackup = false;
                    logger.logInfoWithTracker(
                            trackingId,
                            targetTable,
                            String.format(
                                    "Will skip backup for table %s at this run. Calculated next backup time is %s and this run is %s",
                                    targetTable.toSqlString(),
                                    takeBackupTuple.y(),
                                    referencePoint));
                }
            }
        }
        return takeBackup;
    }

    public static boolean isBackupTime(
            boolean isForceRun, boolean isBackupCronTime, boolean isTableCreatedBeforeTimeTravel) {
        // table must have enough history to use the time travel feature.
        // In addition to that, the run has to be a force run or the backup is due based on the
        // backup
        // cron

        return isTableCreatedBeforeTimeTravel && (isForceRun || isBackupCronTime);
    }

    /**
     * Checks if a cron expression next trigger time has already passed given a reference point in
     * time (e.g. now)
     *
     * @param cronExpression
     * @param lastBackupAtTs
     * @param referencePoint
     * @return Tuple of yes/no and computed next cron expression
     */
    public static Tuple<Boolean, LocalDateTime> getCronNextTrigger(
            String cronExpression, Timestamp lastBackupAtTs, Timestamp referencePoint) {

        CronExpression cron = CronExpression.parse(cronExpression);

        LocalDateTime nowDt =
                LocalDateTime.ofEpochSecond(referencePoint.getSeconds(), 0, ZoneOffset.UTC);

        LocalDateTime lastBackupAtDt =
                LocalDateTime.ofEpochSecond(lastBackupAtTs.getSeconds(), 0, ZoneOffset.UTC);

        // get next execution date based on the last backup date
        LocalDateTime nextExecutionDt = cron.next(lastBackupAtDt);

        return Tuple.of(nextExecutionDt.isBefore(nowDt), nextExecutionDt);
    }

    public Tuple<SnapshoterRequest, SnapshoterRequest> prepareSnapshotRequests(
            BackupPolicyAndState backupPolicy, ConfiguratorRequest request) {

        SnapshoterRequest bqSnapshotRequest = null;
        SnapshoterRequest gcsSnapshotRequest = null;

        if (backupPolicy.getMethod().equals(BackupMethod.BIGQUERY_SNAPSHOT)
                || backupPolicy.getMethod().equals(BackupMethod.BOTH)) {
            bqSnapshotRequest =
                    new SnapshoterRequest(
                            request.getTargetTable(),
                            request.getRunId(),
                            request.getTrackingId(),
                            request.isDryRun(),
                            backupPolicy);
        }

        if (backupPolicy.getMethod().equals(BackupMethod.GCS_SNAPSHOT)
                || backupPolicy.getMethod().equals(BackupMethod.BOTH)) {
            gcsSnapshotRequest =
                    new SnapshoterRequest(
                            request.getTargetTable(),
                            request.getRunId(),
                            request.getTrackingId(),
                            request.isDryRun(),
                            backupPolicy);
        }

        return Tuple.of(bqSnapshotRequest, gcsSnapshotRequest);
    }

    public Tuple<String, BackupPolicy> findFallbackBackupPolicy(
            FallbackBackupPolicy fallbackBackupPolicy, TableSpec tableSpec, String runId)
            throws IOException {

        BackupPolicy tableLevel =
                fallbackBackupPolicy.getTableOverrides().get(tableSpec.toSqlString());
        if (tableLevel != null) {
            return Tuple.of("table", tableLevel);
        }

        BackupPolicy datasetLevel =
                fallbackBackupPolicy
                        .getDatasetOverrides()
                        .get(
                                String.format(
                                        "%s.%s", tableSpec.getProject(), tableSpec.getDataset()));
        if (datasetLevel != null) {
            return Tuple.of("dataset", datasetLevel);
        }

        BackupPolicy projectLevel =
                fallbackBackupPolicy.getProjectOverrides().get(tableSpec.getProject());
        if (projectLevel != null) {
            return Tuple.of("project", projectLevel);
        }

        // API CALL (or cache)
        Tuple<String, String> folderLookupTuple =
                resourceScanner.getParentFolderId(tableSpec.getProject(), runId);
        if (folderLookupTuple != null) {

            String folderId = folderLookupTuple.x();
            BackupPolicy folderLevel = fallbackBackupPolicy.getFolderOverrides().get(folderId);

            if (folderLevel != null) {
                // source is folder-cache or folder-api to trace and debug cache performance
                return Tuple.of(
                        String.format("folder-from-%s", folderLookupTuple.y()), folderLevel);
            }
        }

        // else return the global default policy
        return Tuple.of("default", fallbackBackupPolicy.getDefaultPolicy());
    }
}
