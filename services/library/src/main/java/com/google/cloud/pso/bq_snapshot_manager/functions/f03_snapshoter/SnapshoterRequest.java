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

package com.google.cloud.pso.bq_snapshot_manager.functions.f03_snapshoter;


import com.google.cloud.pso.bq_snapshot_manager.entities.TableOperationRequestResponse;
import com.google.cloud.pso.bq_snapshot_manager.entities.TableSpec;
import com.google.cloud.pso.bq_snapshot_manager.entities.backup_policy.BackupPolicyAndState;
import com.google.common.base.Objects;

public class SnapshoterRequest extends TableOperationRequestResponse {

  private final BackupPolicyAndState backupPolicyAndState;

  public SnapshoterRequest(
      TableSpec targetTable,
      String runId,
      String trackingId,
      boolean isDryRun,
      BackupPolicyAndState backupPolicyAndState) {
    super(targetTable, runId, trackingId, isDryRun);
    this.backupPolicyAndState = backupPolicyAndState;
  }

  public BackupPolicyAndState getBackupPolicyAndState() {
    return backupPolicyAndState;
  }

  @Override
  public String toString() {
    return "SnapshoterRequest{" + "backupPolicy=" + backupPolicyAndState + "} " + super.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    SnapshoterRequest that = (SnapshoterRequest) o;
    return Objects.equal(backupPolicyAndState, that.backupPolicyAndState);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), backupPolicyAndState);
  }

  public String computeBackupOperationProject() {
    // if the backup policy specifies a project to run the backup operations on, use it.
    // Otherwise,
    // use the source table project
    return this.backupPolicyAndState.getBackupOperationProject() != null
        ? this.backupPolicyAndState.getBackupOperationProject()
        : this.getTargetTable().getProject();
  }
}
