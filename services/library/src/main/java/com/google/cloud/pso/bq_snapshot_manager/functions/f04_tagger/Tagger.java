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
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.BackupPolicyAndState;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.BackupState;
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

  public Tagger(
      TaggerConfig config,
      BackupPolicyService backupPolicyService,
      PersistentSet persistentSet,
      String persistentSetObjectPrefix,
      Integer functionNumber) {
    this.config = config;
    this.backupPolicyService = backupPolicyService;
    this.persistentSet = persistentSet;
    this.persistentSetObjectPrefix = persistentSetObjectPrefix;

    logger =
        new LoggingHelper(
            Tagger.class.getSimpleName(),
            functionNumber,
            config.getProjectId(),
            config.getApplicationName());
  }

  /**
   * @param request
   * @param pubSubMessageId
   * @return The backup policy attached to the target table
   * @throws NonRetryableApplicationException
   */
  public TaggerResponse execute(TaggerRequest request, String pubSubMessageId)
      throws NonRetryableApplicationException, RetryableApplicationException, IOException {

    // run common service start logging and checks
    Utils.runServiceStartRoutines(
        logger,
        request,
        persistentSet,
        // This service might be called twice in case of the "Both" backup method. We need to
        // differentiate the key
        String.format("%s/%s", persistentSetObjectPrefix, request.getAppliedBackupMethod()),
        request.getTrackingId());

    BackupState updatedState =
        new BackupState(
            request.getLastBackUpAt(),
            request.getAppliedBackupMethod().equals(BackupMethod.BIGQUERY_SNAPSHOT)
                ? request.getBigQuerySnapshotTableSpec().toResourceUrl()
                : null,
            request.getAppliedBackupMethod().equals(BackupMethod.GCS_SNAPSHOT)
                ? request.getGcsSnapshotUri()
                : null);

    BackupPolicyAndState updatedPolicyAndState =
        new BackupPolicyAndState(request.getBackupPolicyAndState().getPolicy(), updatedState);

    if (!request.isDryRun()) {
      // update the tag
      // API Calls
      backupPolicyService.createOrUpdateBackupPolicyAndStateForTable(
          request.getTargetTable(), updatedPolicyAndState);
    }

    // run common service end logging and adding pubsub message to processed list
    Utils.runServiceEndRoutines(
        logger,
        request,
        persistentSet,
        // This service might be called twice in case of the "Both" backup method. We need to
        // differentiate the key
        String.format("%s/%s", persistentSetObjectPrefix, request.getAppliedBackupMethod()),
        request.getTrackingId());

    return new TaggerResponse(
        request.getTargetTable(),
        request.getRunId(),
        request.getTrackingId(),
        request.isDryRun(),
        request.getBackupPolicyAndState(),
        updatedPolicyAndState);
  }
}
