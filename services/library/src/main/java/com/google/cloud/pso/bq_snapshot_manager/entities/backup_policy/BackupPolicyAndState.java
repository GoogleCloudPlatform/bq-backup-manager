
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

package com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy;

import com.google.cloud.Timestamp;
import com.google.common.base.Objects;

public class BackupPolicyAndState {

    private BackupPolicy policy;
    private BackupState state;


    public BackupPolicyAndState(BackupPolicy policy, BackupState state) {

        this.policy = policy;
        this.state = state;
    }

    public BackupPolicy getPolicy() {
        return policy;
    }

    public BackupState getState() {
        return state;
    }

    public String getCron() {
        return policy.getCron();
    }

    public BackupMethod getMethod() {
        return policy != null? policy.getMethod(): null;
    }

    public TimeTravelOffsetDays getTimeTravelOffsetDays() {
        return policy != null? policy.getTimeTravelOffsetDays(): null;
    }

    public Double getBigQuerySnapshotExpirationDays() {
        return policy != null? policy.getBigQuerySnapshotExpirationDays(): null;
    }

    public String getBackupStorageProject() {
        return policy != null? policy.getBackupStorageProject(): null;
    }

    public String getBackupOperationProject() {
        return policy != null? policy.getBackupOperationProject(): null;
    }

    public String getBigQuerySnapshotStorageDataset() {
        return policy != null? policy.getBigQuerySnapshotStorageDataset(): null;
    }

    public String getGcsSnapshotStorageLocation() {
        return policy != null? policy.getGcsSnapshotStorageLocation(): null;
    }

    public GCSSnapshotFormat getGcsExportFormat() {
        return policy != null? policy.getGcsExportFormat(): null;
    }

    public String getGcsCsvDelimiter() {
        return policy != null? policy.getGcsCsvDelimiter(): null;
    }

    public Boolean getGcsCsvExportHeader() {
        return policy != null? policy.getGcsCsvExportHeader(): null;
    }

    public Boolean getGcsUseAvroLogicalTypes() {
        return policy != null? policy.getGcsUseAvroLogicalTypes(): null;
    }

    public BackupConfigSource getConfigSource() {
        return policy != null? policy.getConfigSource(): null;
    }

    public Timestamp getLastBackupAt() {
        return state != null? state.getLastBackupAt(): null;
    }

    public String getLastBqSnapshotStorageUri() {
        return state != null? state.getLastBqSnapshotStorageUri(): null;
    }

    public String getLastGcsSnapshotStorageUri() {
        return state != null? state.getLastGcsSnapshotStorageUri(): null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BackupPolicyAndState that = (BackupPolicyAndState) o;
        return Objects.equal(policy, that.policy) && Objects.equal(state, that.state);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(policy, state);
    }

    @Override
    public String toString() {
        return "BackupPolicyAndState{" +
                "policy=" + policy +
                ", state=" + state +
                '}';
    }
}
