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

package com.google.cloud.pso.bq_snapshot_manager.functions.f04_tagger;

import com.google.cloud.pso.bq_snapshot_manager.entities.NonRetryableApplicationException;
import com.google.cloud.pso.bq_snapshot_manager.entities.RetryableApplicationException;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.BackupMethod;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.BackupPolicy;
import com.google.cloud.pso.bq_snapshot_manager.functions.f03_snapshoter.GCSSnapshoter;
import com.google.cloud.pso.bq_snapshot_manager.helpers.LoggingHelper;
import com.google.cloud.pso.bq_snapshot_manager.helpers.Utils;
import com.google.cloud.pso.bq_snapshot_manager.services.backup_policy.BackupPolicyService;
import com.google.cloud.pso.bq_snapshot_manager.services.set.PersistentSet;

import java.io.IOException;

public class Tagger {

    private final LoggingHelper logger;

    private final TaggerConfig config;
    private final BackupPolicyService backupPolicyService;
    private final PersistentSet persistentSet;
    private final String persistentSetObjectPrefix;

    private final Integer functionNumber;

    public Tagger(TaggerConfig config, BackupPolicyService backupPolicyService, PersistentSet persistentSet, String persistentSetObjectPrefix, Integer functionNumber) {
        this.config = config;
        this.backupPolicyService = backupPolicyService;
        this.persistentSet = persistentSet;
        this.persistentSetObjectPrefix = persistentSetObjectPrefix;
        this.functionNumber = functionNumber;

        logger = new LoggingHelper(
                Tagger.class.getSimpleName(),
                functionNumber,
                config.getProjectId(),
                config.getApplicationName()
        );
    }

    /**
     *
     * @param request
     * @param pubSubMessageId
     * @return The backup policy attached to the target table
     * @throws NonRetryableApplicationException
     */
    public TaggerResponse execute(
            TaggerRequest request,
            String pubSubMessageId
    ) throws NonRetryableApplicationException, RetryableApplicationException, IOException {

        // run common service start logging and checks
        Utils.runServiceStartRoutines(
                logger,
                request,
                persistentSet,
                persistentSetObjectPrefix,
                pubSubMessageId
        );

        // We want to process exactly one tagging request per table at a time to avoid race condition on creating/updating tags with the "Both" backup method
        String trackerFlagFileName = String.format("%s/%s", persistentSetObjectPrefix, request.getTrackingId());
        if (persistentSet.contains(trackerFlagFileName)) {
            // log error and ACK and return
            String msg = String.format("Tracking ID '%s' for table '%s' is already being processed by the service at the moment. Please retry later.",
                    request.getTrackingId(),
                    request.getTargetTable().toSqlString()
            );
            throw new RetryableApplicationException(msg);
        }else{
            // add the lock if it doesn't exist
            persistentSet.add(trackerFlagFileName);
        }

        try{

            // prepare the backup policy tag
            BackupPolicy originalBackupPolicy = request.getBackupPolicy();
            BackupPolicy.BackupPolicyBuilder updatedPolicyBuilder = BackupPolicy.BackupPolicyBuilder.from(originalBackupPolicy);

            // set the last_xyz fields
            updatedPolicyBuilder.setLastBackupAt(request.getLastBackUpAt());

            if(request.getAppliedBackupMethod().equals(BackupMethod.BIGQUERY_SNAPSHOT)){
                updatedPolicyBuilder.setLastBqSnapshotStorageUri(request.getBigQuerySnapshotTableSpec().toResourceUrl());
            }

            if(request.getAppliedBackupMethod().equals(BackupMethod.GCS_SNAPSHOT)){
                updatedPolicyBuilder.setLastGcsSnapshotStorageUri(request.getGcsSnapshotUri());
            }

            BackupPolicy upDatedBackupPolicy = updatedPolicyBuilder.build();

            if(!request.isDryRun()){
                // update the tag
                // API Calls
                backupPolicyService.createOrUpdateBackupPolicyForTable(
                        request.getTargetTable(),
                        upDatedBackupPolicy
                );
            }

            // run common service end logging and adding pubsub message to processed list
            Utils.runServiceEndRoutines(
                    logger,
                    request,
                    persistentSet,
                    persistentSetObjectPrefix,
                    pubSubMessageId
            );

            return new TaggerResponse(
                    request.getTargetTable(),
                    request.getRunId(),
                    request.getTrackingId(),
                    request.isDryRun(),
                    upDatedBackupPolicy,
                    originalBackupPolicy
            );

        }finally {

            // always remove the tracker lock to allow other calls on the same table in the same run, either retried ones or for another backup method
            persistentSet.remove(trackerFlagFileName);
        }
    }
}