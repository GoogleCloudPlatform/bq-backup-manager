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
import com.google.cloud.pso.bq_snapshot_manager.entities.TableOperationRequestResponse;
import com.google.cloud.pso.bq_snapshot_manager.entities.TableSpec;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.BackupPolicy;
import com.google.cloud.pso.bq_snapshot_manager.functions.f03_snapshoter.SnapshoterRequest;
import com.google.cloud.pso.bq_snapshot_manager.services.pubsub.PubSubPublishResults;

public class ConfiguratorResponse extends TableOperationRequestResponse {

    private final BackupPolicy backupPolicy;
    private final Timestamp refTs;
    private final boolean isBackupTime;
    private final SnapshoterRequest bqSnapshoterRequest;
    private final SnapshoterRequest gcsSnapshoterRequest;
    private final PubSubPublishResults bigQueryBackupPublishingResults;
    private final PubSubPublishResults gcsBackupPublishingResults;

    public ConfiguratorResponse(TableSpec targetTable, String runId, String trackingId, boolean isDryRun, BackupPolicy backupPolicy, Timestamp refTs, boolean isBackupTime, SnapshoterRequest bqSnapshoterRequest, SnapshoterRequest gcsSnapshoterRequest, PubSubPublishResults bigQueryBackupPublishingResults, PubSubPublishResults gcsBackupPublishingResults) {
        super(targetTable, runId, trackingId, isDryRun);
        this.backupPolicy = backupPolicy;
        this.refTs = refTs;
        this.isBackupTime = isBackupTime;
        this.bqSnapshoterRequest = bqSnapshoterRequest;
        this.gcsSnapshoterRequest = gcsSnapshoterRequest;
        this.bigQueryBackupPublishingResults = bigQueryBackupPublishingResults;
        this.gcsBackupPublishingResults = gcsBackupPublishingResults;
    }

    public BackupPolicy getBackupPolicy() {
        return backupPolicy;
    }

    public Timestamp getRefTs() {
        return refTs;
    }

    public boolean isBackupTime() {
        return isBackupTime;
    }

    public SnapshoterRequest getBigQuerySnapshoterRequest() {
        return bqSnapshoterRequest;
    }

    public SnapshoterRequest getGcsSnapshoterRequest() {
        return gcsSnapshoterRequest;
    }

    public PubSubPublishResults getBigQueryBackupPublishingResults() {
        return bigQueryBackupPublishingResults;
    }

    public PubSubPublishResults getGcsBackupPublishingResults() {
        return gcsBackupPublishingResults;
    }
}
